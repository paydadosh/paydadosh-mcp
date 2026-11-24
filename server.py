"""PaydaDosh — открытый MCP-сервер (Model Context Protocol) ингушско-русского словаря, `https://mcp.paydadosh.ru/mcp`.

Маленькая отдельная служба: слушает свой порт (по умолчанию 127.0.0.1:8010, снаружи — через обратный прокси),
данные берёт у сайта PaydaDosh по его внутреннему HTTP (`MCP_SITE_API`) и ничего из кода сайта не импортирует:

* `GET  /api/mcp-feed/v1/search`     — словарь;
* `GET  /api/mcp-feed/v1/razbor`     — разбор слова и таблица форм с ярусом достоверности формы;
* `POST /api/mcp-feed/v1/spell`      — проверка орфографии;
* `GET  /api/mcp-feed/v1/context`    — контекст перевода (словарь, корпус, разговорник, грамматика, ответы носителей);
* `POST /api/mcp-feed/v1/check-text` — каждое слово текста: известно ли словарю и чья это форма;
* `GET  /api/app/v1/corpus`          — примеры из корпуса (первая страница, берём ≤ 5 строк).

Рассуждает модель пользователя — сервер отдаёт только данные, никаких LLM на нашей стороне. Наружу не выходит
ничего пишущего и никаких имён модераторов. Транспорт — Streamable HTTP без состояния (`stateless_http=True`,
`json_response=True`): каждый запрос самодостаточен, перезапуск службы не рвёт ничьих сессий.

Журнал вызовов — `data/mcp_calls-ГГГГ-ММ-ДД.jsonl` (файл на день, хранится `MCP_LOG_RETENTION_DAYS` = 90 дней):
время, инструмент, запрос (≤ 300 знаков у поиска и контекста, ≤ 64 у остальных; у проверок текста — только
незнакомые словарю слова и длина), found, мс, клиент, хэш IP с солью `MCP_IP_SALT`. Отчёт — `mcp_stats.py`.

Запуск: `python server.py` (или `uvicorn server:app --host 127.0.0.1 --port 8010`).
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import ipaddress
import json
import logging
import os
import re
import secrets
import threading
import time
from collections import OrderedDict, deque
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, Any, Awaitable, Callable, Literal
from urllib.parse import quote

import httpx
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.server.fastmcp.prompts.base import Prompt
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import CallToolResult, Icon, ResourceLink, TextContent, ToolAnnotations
from pydantic import Field
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response

VERSION = "1.1.1"
HERE = Path(__file__).resolve().parent
log = logging.getLogger("paydadosh.mcp")


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


SITE_API = _env("MCP_SITE_API", "http://127.0.0.1:8000").rstrip("/")      # внутренний адрес сайта
PUBLIC_SITE = _env("MCP_PUBLIC_SITE", "https://paydadosh.ru").rstrip("/")  # ссылки в ответах
HOST = _env("MCP_HOST", "127.0.0.1")
PORT = int(_env("MCP_PORT", "8010"))
LOG_DIR = Path(_env("MCP_LOG_DIR", str(HERE / "data")))
RETENTION_DAYS = int(_env("MCP_LOG_RETENTION_DAYS", "90"))
API_TIMEOUT = float(_env("MCP_API_TIMEOUT", "8"))           # секунд на вызов сайта
CONTEXT_TIMEOUT = float(_env("MCP_CONTEXT_TIMEOUT", "15"))  # контекст перевода собирается дольше
SITE_CONCURRENCY = int(_env("MCP_SITE_CONCURRENCY", "2"))   # одновременных вызовов сайта, не больше
CONTEXT_PER_MINUTE = int(_env("MCP_CONTEXT_PER_MINUTE", "20"))  # translation_context с одного адреса
# X-Real-IP верим только от своего прокси (служба может слушать сеть, не только 127.0.0.1); иначе — адрес соединения
TRUSTED_PROXIES = {x.strip() for x in _env("MCP_TRUSTED_PROXIES", "127.0.0.1").split(",") if x.strip()}
# Общие адреса облачных клиентов: все пользователи Claude.ai приходят с этих сетей — лимит «на адрес» для них выше
SHARED_EGRESS = [ipaddress.ip_network(x.strip(), strict=False) for x in
                 _env("MCP_SHARED_EGRESS_CIDRS", "160.79.104.0/21,153.61.208.0/20").split(",") if x.strip()]
SHARED_CONTEXT_PER_MINUTE = int(_env("MCP_SHARED_CONTEXT_PER_MINUTE", "200"))
LOG_MAX_BYTES_PER_DAY = int(float(_env("MCP_LOG_MAX_MB_PER_DAY", "50")) * 1024 * 1024)
QUEUE_TIMEOUT = 10.0                                        # сколько ждать своей очереди к сайту
CACHE_TTL = 600.0                                           # кэш одинаковых вызовов — 10 минут
CACHE_MAX = 2000
MAX_RESULT_BYTES = 20_000                                   # ответ инструмента ≤ ~20 КБ
MAX_BODY_BYTES = 64 * 1024                                  # тело запроса MCP
TEXT_MAX = 1000                                             # знаков текста на проверку
EXAMPLES_MAX = 5                                            # корпус: не больше 5 предложений, 1-я страница
EXAMPLE_CHARS = 300
LOG_Q_LONG = {"search_dictionary", "translation_context"}   # у них запрос в журнале — до 300 знаков
LINK_PROTOCOL = "2025-06-18"                                # с этой версии протокола есть resource_link

INSTRUCTIONS = """\
PaydaDosh — Ingush dictionary (paydadosh.ru): an Ingush–Russian dictionary with tens of thousands of \
headwords merged from dozens of printed dictionaries, Ozdoev's Russian–Ingush dictionary, a corpus of \
Ingush texts with Russian translations, a morphological analyzer and a spell-checker.

How to answer:
1. Phrase or "how do you say…": call translation_context first — native-speaker corrections there are the \
reference, phrasebook and corpus pairs come next; single words — search_dictionary; forms — get_paradigm.
2. Take Ingush words and forms ONLY from tool results. found=false means PaydaDosh has no such word: say so, \
do not invent or "reconstruct" a form. Check your own Ingush text with check_ingush_text before answering.
3. Give the user a paydadosh.ru link (`url`) for EVERY Ingush word or form in your answer, and cite the \
source code (БДИМТ09, КУР05, ОЗД80…). If an entry or meaning has exactly ONE code in `sources`, say so right \
next to it, e.g. «(только в БДИМТ09)»: one printed dictionary is weaker evidence than several.
4. These are dictionary data, not a native-speaker check: forms with tier=rule and analyses with \
confidence=hypothesis are unverified — say so. Palochka is Ӏ (U+04C0); in queries I, 1, l or | also work.
5. If the user says a form or translation is wrong, give them the item's `report_url` — the error form on \
the paydadosh.ru word page («сообщить об ошибке на paydadosh.ru»); a word without a card — the site's \
feedback form https://paydadosh.ru/#feedback-form. Never report anything yourself: this server is read-only \
and accepts no reports.
6. Definitions and notes are in Russian, as on the site. The prompt "ingush_assistant" has the full rules.

Tools: search_dictionary, analyze_word, get_paradigm, find_examples, translation_context, \
check_ingush_text, check_spelling.\
"""

# Подсказка «Ингушский помощник»: сжатые правила помощника сайта (порядок источников и главные ловушки).
ASSISTANT_RULES = """\
Ты отвечаешь на вопросы об ингушском языке, опираясь ТОЛЬКО на инструменты PaydaDosh. Лучше честно сказать
«в словаре этого нет», чем выдумать форму.

ПОРЯДОК РАБОТЫ
1. Перевод фразы, «как сказать…» — сначала translation_context. В нём: правки носителей (эталон — бери
   дословно), разговорник (готовые фразы; повод в occasion должен совпадать с запросом), пары из корпуса,
   словарь, грамматика, ответы носителей в «Хаттараш».
2. Ингушская фраза → русский: сначала ищи параллель в parallel / phrasebook контекста — её перевод и есть
   ответ; нет параллели — переводи смыслом фразы целиком, потом краткий разбор по словам.
3. Отдельные слова — search_dictionary (оба направления); для употребления и готовых сочетаний со словом —
   ещё translation_context (пары корпуса, разговорник, правки носителей); что за форма — analyze_word; все
   формы — get_paradigm (бери форму из таблицы, а не склоняй сам; форму другого класса — из ряда cls).
4. Свой ингушский текст перед ответом прогони через check_ingush_text: слово с verdict=unverified — замени
   подтверждённым или прямо скажи, что оно не подтверждено. Лемма там — первое чтение анализатора
   (справка); значения и ссылки для ответа бери из search_dictionary.
5. found=false — так и скажи, предложи близкие из suggestions.

В ОТВЕТЕ
- Каждое ингушское слово и форму давай со ссылкой на его страницу paydadosh.ru (поле url).
- Указывай источник: код издания (БДИМТ09, КУР05, ОЗД80…), «разговорник», «Хаттараш», «корпус».
- У статьи или значения ровно один код в sources — так и скажи рядом: «(только в БДИМТ09)». Одно издание —
  слабее, чем несколько; если у омонимов разное число источников, это видно человеку.
- Формы с tier=rule и разборы confidence=hypothesis помечай как непроверенные.
- Ингушские слова пиши с палочкой Ӏ (не латинской I и не цифрой 1).
- Человек говорит, что форма или перевод неверны, — дай ему report_url этого слова:
  «сообщить об ошибке на paydadosh.ru» (форма на карточке слова, сообщение читает редакция);
  у слова без карточки — форма отзыва https://paydadosh.ru/#feedback-form.
  Сам никуда ничего не отправляй и не обещай, что ошибку исправят: инструменты PaydaDosh только читают.

ЛОВУШКИ
- Класс глагола = класс абсолютива. Переходный глагол с эргативом (аз, Ӏа, цо) — класс по ОБЪЕКТУ
  («аз кинижка хьаийцад», кинижка — д); непереходный — по субъекту («со вахар» / «со яхар»).
- «Ты» при переходном глаголе — Ӏа (эргатив); хьо — при непереходном.
- Аффективы (хет, хов, гу, деза, мага) — субъект в дативе: «сона хов», «мишта хет хьона?»,
  «я тебя люблю» = «сона веза хьо» (м.) / «сона еза хьо» (ж.). Но кхета — не аффектив: «со кхета».
- Датив сона/хьона (кому дать, кому кажется) ≠ направительный сога/хьога (кому сказать). «Приду к тебе» =
  «хьо волча воагӀаргва», «был у тебя» = «хьо волча вар» — не «хьога».
- Мн. ч. тхо / вай / шо — по классу д («тхо долха»), уж — по классу б.
- Связка — отдельным словом: ва / я / ба / да («со кӀаьдвенна ва»).
- Омонимы — не смешивай формы двух значений: ха (время / охрана / знать), цӀи (огонь / имя), да (отец /
  связка), мала (пить / кто), кхера (бояться / камень), гӀа (шаг / сон).
- Чеченские формы (ду / бу / ю / ву, хӀун, муха, суна / хьуна) — не ингушские; дай ингушский
  эквивалент (да / ба / я / ва, фу, мишта, сона / хьона) и скажи, что вопрос был по-чеченски.
- Заимствования (ишкол, телефон, тика «магазин») — полноправные ингушские слова, не «не найдено».
- Порядок слов SOV: глагол в конце.
"""

_READ_ONLY = dict(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)


# ── палочка и вид ответа ───────────────────────────────────────────────────────────────────────
# Двойник палочки заменяем, только если он ПРИЖАТ к кириллице: «кIант», «г1улакх» чиним, «1941 шу» и латиницу
# не трогаем (то же правило, что у сайта для показа). Вертикальная черта в запросе человека — тоже палочка.
_CYR = "Ѐ-ӿԀ-ԯ"
PALOCHKA = "Ӏ"
_PAL_LETTER = re.compile(rf"(?<=[{_CYR}])[IliіІ|]|(?<![A-Za-z])[IliіІ|](?=[{_CYR}])")
_PAL_DIGIT = re.compile(rf"(?<=[{_CYR}])1(?!\d)|(?<!\d)1(?=[{_CYR}])")
_INVISIBLE = re.compile("[\u200b-\u200f\u2060\ufeff\u00ad]")   # нулевой ширины, мягкий перенос


def normalize_query(text: str) -> str:
    """Вход инструмента: пробелы, невидимые знаки, палочка в любом написании → Ӏ."""
    s = _INVISIBLE.sub("", text or "")
    s = " ".join(s.split())
    s = s.replace("ӏ", PALOCHKA)
    s = _PAL_LETTER.sub(PALOCHKA, s)
    return _PAL_DIGIT.sub(PALOCHKA, s)


_KEEP_AS_IS = {"url", "path", "report_url"}   # адреса не трогаем: слаг страницы сайта пишется строчной ӏ


def display(obj: Any) -> Any:
    """Палочка в ответе — одна, Ӏ (U+04C0), как в заголовках словаря; в нормализованных полях сайта лежит ӏ."""
    if isinstance(obj, str):
        return obj.replace("ӏ", PALOCHKA)
    if isinstance(obj, list):
        return [display(x) for x in obj]
    if isinstance(obj, dict):
        return {k: (v if k in _KEEP_AS_IS else display(v)) for k, v in obj.items()}
    return obj


def _clip(text: Any, cap: int) -> str:
    s = " ".join(str(text or "").split())
    return s if len(s) <= cap else s[: cap - 1].rstrip() + "…"


def _dumps(obj: Any) -> str:
    # Компактно: текст ответа читает модель, отступы — это лишние токены (и вдвое больше байт).
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _size(obj: Any) -> int:
    return len(_dumps(obj).encode("utf-8"))


def _lists(obj: Any):
    if isinstance(obj, list):
        yield obj
        for x in obj:
            yield from _lists(x)
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _lists(v)


def fit(obj: dict[str, Any], limit: int = MAX_RESULT_BYTES) -> dict[str, Any]:
    """Ответ не больше `limit` байт: укорачиваем самый тяжёлый список с конца, ставим `truncated`."""
    if _size(obj) <= limit:
        return obj
    obj = copy.deepcopy(obj)
    obj["truncated"] = True
    for _ in range(500):
        if _size(obj) <= limit:
            return obj
        heavy = max((lst for lst in _lists(obj) if len(lst) > 1), key=_size, default=None)
        if heavy is None:
            break
        heavy.pop()
    for key, val in list(obj.items()):   # последний рубеж: длинные строки
        if isinstance(val, str) and len(val) > 500:
            obj[key] = _clip(val, 500)
    return obj


def site_url(path: str) -> str:
    """Адрес страницы сайта. Слаг бывает кириллическим («/word/109286-кӏант») — кодируем в ASCII."""
    path = path if path.startswith("/") else "/" + path
    return PUBLIC_SITE + quote(path, safe="/-._~%?=&#")


def page_url(page: str, q: str) -> str:
    return f"{PUBLIC_SITE}{page}?q={quote(q, safe='')}" if q else PUBLIC_SITE + page


# Обратная связь в первом выпуске — только ссылкой: у каждой карточки слова на сайте есть форма
# «Сообщить об ошибке в карточке» (word.html, section#word-report-error; сообщение уходит редакции через
# /api/feedback). Сам сервер ничего не отправляет — пишущего инструмента нет (фаза 2 плана).
REPORT_ANCHOR = "#word-report-error"


def report_url(url: str) -> str:
    """Адрес формы ошибки на карточке слова; у страницы, которая не карточка (/razbor, поиск), — пусто."""
    if not url.startswith(PUBLIC_SITE + "/word/"):
        return ""
    return url.split("#", 1)[0] + REPORT_ANCHOR


def with_report(item: dict[str, Any]) -> dict[str, Any]:
    """Ставит report_url рядом с url, если url — карточка слова."""
    rep = report_url(item.get("url") or "")
    if rep:
        item["report_url"] = rep
    return item


# Инструмент отдаёт текст (компактный JSON — его читает модель), строку источника со ссылкой и тот же объект
# структурой; клиентам протокола 2025-06-18+ — ещё resource_link на страницу сайта.
ToolOut = Annotated[CallToolResult, dict[str, Any]]


def source_line(data: dict[str, Any]) -> str:
    return f"Источник: PaydaDosh — {data.get('url') or PUBLIC_SITE}"


def as_result(data: dict[str, Any], links: bool = False) -> CallToolResult:
    content: list[Any] = [TextContent(type="text", text=_dumps(data)),
                          TextContent(type="text", text=source_line(data))]
    if links and data.get("url"):
        content.append(ResourceLink(type="resource_link", uri=data["url"], name="paydadosh-page",
                                    title="PaydaDosh", mimeType="text/html",
                                    description="Страница на paydadosh.ru"))
    return CallToolResult(content=content, structuredContent=data)


# ── доступ к сайту ─────────────────────────────────────────────────────────────────────────────
class UpstreamError(Exception):
    """Сайт не ответил или ответил ошибкой — это не «не найдено», и так модели и говорим."""


class SiteAPI:
    def __init__(self, base: str = SITE_API, timeout: float = API_TIMEOUT,
                 transport: httpx.AsyncBaseTransport | None = None, feed_key: str | None = None,
                 concurrency: int = SITE_CONCURRENCY) -> None:
        headers = {"User-Agent": f"paydadosh-mcp/{VERSION}", "Accept": "application/json"}
        key = _env("MCP_FEED_KEY") if feed_key is None else feed_key
        if key:
            headers["X-MCP-Feed-Key"] = key
        self._client = httpx.AsyncClient(base_url=base, timeout=httpx.Timeout(timeout), headers=headers,
                                         transport=transport, trust_env=False)
        self._sem = asyncio.Semaphore(max(1, concurrency))

    async def _call(self, method: str, path: str, timeout: float | None = None, **kw: Any) -> dict[str, Any]:
        # Очередь к сайту: у воркера сайта одно соединение с базой, параллельные тяжёлые вызовы встают
        # друг за другом (замер: 4 одновременно — p50 3,4 с против 0,2–0,9 с по одному).
        try:
            await asyncio.wait_for(self._sem.acquire(), timeout=QUEUE_TIMEOUT)
        except asyncio.TimeoutError as exc:
            raise UpstreamError("busy") from exc
        try:
            extra = {"timeout": httpx.Timeout(timeout)} if timeout else {}
            resp = await self._client.request(method, path, **extra, **kw)
        except httpx.TimeoutException as exc:
            raise UpstreamError("timeout") from exc
        except httpx.HTTPError as exc:
            raise UpstreamError(type(exc).__name__) from exc
        finally:
            self._sem.release()
        if resp.status_code != 200:
            raise UpstreamError(f"HTTP {resp.status_code}")
        try:
            data = resp.json()
        except ValueError as exc:
            raise UpstreamError("bad JSON") from exc
        if not isinstance(data, dict):
            raise UpstreamError("bad JSON")
        return data

    async def get(self, path: str, params: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        return await self._call("GET", path, timeout=timeout, params=params)

    async def post(self, path: str, payload: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        return await self._call("POST", path, timeout=timeout, json=payload)

    async def aclose(self) -> None:
        await self._client.aclose()


class TTLCache:
    def __init__(self, ttl: float = CACHE_TTL, max_items: int = CACHE_MAX) -> None:
        self.ttl, self.max_items = ttl, max_items
        self._d: OrderedDict[str, tuple[float, Any]] = OrderedDict()

    def get(self, key: str) -> Any:
        item = self._d.get(key)
        if item is None:
            return None
        if item[0] < time.monotonic():
            self._d.pop(key, None)
            return None
        self._d.move_to_end(key)
        return item[1]

    def set(self, key: str, value: Any) -> None:
        self._d[key] = (time.monotonic() + self.ttl, value)
        self._d.move_to_end(key)
        while len(self._d) > self.max_items:
            self._d.popitem(last=False)

    def clear(self) -> None:
        self._d.clear()


class RateLimiter:
    """Скользящее окно в минуту на ключ (хэш IP). Нужен тяжёлому translation_context — сверх лимита nginx."""

    MAX_KEYS = 10_000

    def __init__(self, per_minute: int, shared_per_minute: int | None = None) -> None:
        self.per_minute = per_minute
        self.shared_per_minute = shared_per_minute or per_minute
        self._hits: OrderedDict[str, deque] = OrderedDict()

    def allow(self, key: str, shared: bool = False) -> bool:
        """`shared` — адрес из общей сети облачного клиента (за ним много людей): лимит выше."""
        now = time.monotonic()
        key = key or "anon"
        dq = self._hits.get(key)
        if dq is None:
            dq = self._hits[key] = deque()
        self._hits.move_to_end(key)
        while dq and dq[0] <= now - 60:
            dq.popleft()
        limit = self.shared_per_minute if shared else self.per_minute
        if len(dq) >= limit:
            return False
        dq.append(now)
        while len(self._hits) > self.MAX_KEYS:   # ограниченная память: выбрасываем самый давний ключ
            self._hits.popitem(last=False)
        return True


# ── журнал вызовов ─────────────────────────────────────────────────────────────────────────────
_DAY_FILE = re.compile(r"^mcp_calls-(\d{4}-\d{2}-\d{2})\.jsonl$")


class Journal:
    """Строка на вызов в JSONL, файл на день, старше `retention_days` — удаляется. IP — только хэшем с солью;
    соль — из окружения (`MCP_IP_SALT`), в репозитории её нет."""

    def __init__(self, directory: Path = LOG_DIR, salt: str | None = None,
                 retention_days: int = RETENTION_DAYS) -> None:
        self.directory = Path(directory)
        self.retention_days = retention_days
        salt = _env("MCP_IP_SALT") if salt is None else salt
        if not salt:
            # без соли хэши не сравнимы между перезапусками, но IP в открытом виде всё равно не пишем
            log.warning("MCP_IP_SALT не задана: соль для хэша IP — случайная на время работы процесса")
            salt = secrets.token_hex(16)
        self._salt = salt
        self._lock = threading.Lock()
        self._day = ""
        self._day_bytes = 0
        self._day_capped = False
        self.max_bytes_per_day = LOG_MAX_BYTES_PER_DAY

    def ip_hash(self, ip: str) -> str:
        return hashlib.sha256(f"{self._salt}|{ip}".encode()).hexdigest()[:16] if ip else ""

    def path_for(self, day: str) -> Path:
        return self.directory / f"mcp_calls-{day}.jsonl"

    def purge(self, today: date) -> list[Path]:
        """Удалить дневные файлы старше срока хранения. Возвращает удалённые."""
        border = today - timedelta(days=self.retention_days)
        gone = []
        for f in self.directory.glob("mcp_calls-*.jsonl"):
            m = _DAY_FILE.match(f.name)
            if not m:
                continue
            try:
                if date.fromisoformat(m.group(1)) < border:
                    f.unlink()
                    gone.append(f)
            except (ValueError, OSError):
                continue
        return gone

    def write(self, record: dict[str, Any]) -> None:
        now = datetime.now(timezone.utc)
        day = now.strftime("%Y-%m-%d")
        rec = {"ts": now.strftime("%Y-%m-%dT%H:%M:%SZ"), **record}
        line = json.dumps(rec, ensure_ascii=False)
        try:
            with self._lock:
                self.directory.mkdir(parents=True, exist_ok=True)
                if day != self._day:      # раз в сутки — уборка старых файлов
                    self._day = day
                    self.purge(now.date())
                    f = self.path_for(day)
                    self._day_bytes = f.stat().st_size if f.exists() else 0
                    self._day_capped = False
                if self._day_capped:
                    return
                data = line + "\n"
                if self._day_bytes + len(data.encode("utf-8")) > self.max_bytes_per_day:
                    # потолок на день (ревью 24.11.2026): дешёвые запросы не должны забить диск
                    self._day_capped = True
                    data = json.dumps({"ts": rec["ts"], "tool": "journal_capped", "q": "", "found": None},
                                      ensure_ascii=False) + "\n"
                with open(self.path_for(day), "a", encoding="utf-8") as fh:
                    fh.write(data)
                self._day_bytes += len(data.encode("utf-8"))
        except OSError:
            log.warning("журнал MCP не записался", exc_info=True)


_UA_FAMILIES = (
    ("claude", "claude"), ("anthropic", "claude"), ("chatgpt", "chatgpt"), ("openai", "openai"),
    ("cursor", "cursor"), ("windsurf", "windsurf"), ("vscode", "vscode"), ("visual studio code", "vscode"),
    ("inspector", "mcp-inspector"), ("python-httpx", "python-httpx"), ("node", "node"), ("curl", "curl"),
)


def ua_family(ua: str) -> str:
    low = (ua or "").lower()
    for needle, name in _UA_FAMILIES:
        if needle in low:
            return name
    head = re.sub(r"[^\w.\-]+", "", (ua or "").split("/")[0])
    return head[:32] or "unknown"


def client_name(raw: Any) -> str:
    return re.sub(r"[^\w .\-()]+", "", str(raw or ""))[:40].strip()


class Clients:
    """`clientInfo.name` из `initialize`. Сервер без состояния: вызов инструмента приходит отдельным запросом,
    и клиента узнаём по паре (хэш IP, User-Agent) — у облачных клиентов она общая на всех, для счёта
    «какие клиенты» этого хватает."""

    TTL = 6 * 3600
    MAX_KEYS = 5000

    def __init__(self) -> None:
        self._d: OrderedDict[tuple[str, str], tuple[float, str]] = OrderedDict()

    @staticmethod
    def _key(key: tuple[str, str]) -> tuple[str, str]:
        iph, ua = key
        return iph, hashlib.sha256((ua or "").encode("utf-8", "replace")).hexdigest()[:16]

    def remember(self, key: tuple[str, str], name: str) -> None:
        if name:
            k = self._key(key)
            self._d[k] = (time.monotonic() + self.TTL, name)
            self._d.move_to_end(k)
            while len(self._d) > self.MAX_KEYS:   # ограниченная LRU (ревью 24.11.2026)
                self._d.popitem(last=False)

    def lookup(self, key: tuple[str, str]) -> str:
        item = self._d.get(self._key(key))
        return item[1] if item and item[0] > time.monotonic() else ""


class State:
    def __init__(self, api: SiteAPI | None = None, journal: Journal | None = None) -> None:
        self.api = api or SiteAPI()
        self.journal = journal or Journal()
        self.cache = TTLCache()
        self.clients = Clients()
        self.context_limiter = RateLimiter(CONTEXT_PER_MINUTE, SHARED_CONTEXT_PER_MINUTE)


STATE: State | None = None


def state() -> State:
    global STATE
    if STATE is None:
        STATE = State()
    return STATE


def _headers_ip_ua(headers: Any, client_host: str = "") -> tuple[str, str]:
    # X-Real-IP ставит наш обратный прокси (адрес человека). Служба может слушать сеть, а не только 127.0.0.1,
    # поэтому заголовку верим только от адресов MCP_TRUSTED_PROXIES; X-Forwarded-For не читаем вовсе
    # (его левую часть пишет сам клиент).
    ip = (headers.get("x-real-ip") or "").strip() if client_host in TRUSTED_PROXIES else ""
    return ip or client_host, (headers.get("user-agent") or "")[:300]


def is_shared_egress(ip: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return any(addr in net for net in SHARED_EGRESS)


def _request(ctx: Context | None) -> Any:
    try:
        return ctx.request_context.request if ctx is not None else None
    except (ValueError, AttributeError):
        return None


def _request_meta(ctx: Context | None) -> tuple[str, str]:
    req = _request(ctx)
    if req is None:
        return "", ""
    host = req.client.host if getattr(req, "client", None) else ""
    return _headers_ip_ua(req.headers, host)


def wants_links(ctx: Context | None) -> bool:
    """resource_link появился в протоколе 2025-06-18; клиент старше его не ждёт — не шлём."""
    req = _request(ctx)
    version = (req.headers.get("mcp-protocol-version") or "") if req is not None else ""
    return bool(version) and version >= LINK_PROTOCOL


async def run_tool(tool: str, ctx: Context | None, args: dict[str, Any], log_q: str,
                   compute: Callable[[], Awaitable[dict[str, Any]]],
                   log_extra: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
                   limiter: RateLimiter | None = None) -> CallToolResult:
    """Общая обвязка инструмента: кэш 10 минут, лимит, размер ответа, палочка, журнал, ошибки сайта."""
    st = state()
    started = time.perf_counter()
    ip, ua = _request_meta(ctx)
    iph = st.journal.ip_hash(ip)
    client = st.clients.lookup((iph, ua)) or ua_family(ua)
    key = tool + "|" + json.dumps(args, sort_keys=True, ensure_ascii=False)
    cached = st.cache.get(key)
    result: dict[str, Any] | None = cached
    error = ""
    try:
        if result is None:
            if limiter is not None and not limiter.allow(iph, shared=is_shared_egress(ip)):
                error = "rate_limited"
                raise ToolError(f"Rate limit: {limiter.per_minute} {tool} calls per minute. Wait a minute; "
                                "search_dictionary and get_paradigm are not limited this way.")
            result = fit(display(await compute()))
            st.cache.set(key, result)
        return as_result(result, links=wants_links(ctx))
    except UpstreamError as exc:
        error = str(exc)
        raise ToolError(f"PaydaDosh is temporarily unavailable ({error}). Try again in a minute; "
                        "this is not a 'not found' answer.") from exc
    except ToolError:
        error = error or "tool"
        raise
    except Exception as exc:  # ошибка в нашем коде: модели — короткое сообщение, нам — трассировка
        error = "internal"
        log.exception("инструмент %s упал", tool)
        raise ToolError("Internal error in the PaydaDosh MCP server.") from exc
    finally:
        rec: dict[str, Any] = {
            "tool": tool, "q": (log_q or "")[: 300 if tool in LOG_Q_LONG else 64],
            "found": (result or {}).get("found") if not error else None,
            "ms": round((time.perf_counter() - started) * 1000),
            "client": client, "ip": iph, "cached": cached is not None,
        }
        if log_extra is not None and result is not None and not error:
            try:
                rec.update(log_extra(result))
            except Exception:
                pass
        if error:
            rec["error"] = error
        st.journal.write(rec)


# ── ответы инструментов (чистые функции — их и проверяют тесты) ─────────────────────────────────
LEVEL = {"ok": "high", "mid": "medium", "hyp": "hypothesis"}
PROV_RAZBOR = {"ok": "verified", "pub": "published", "draft": "draft"}
PROV_CORPUS = {"verified": "verified", "published": "published", "draft": "draft", "": "none"}
TIER_LEGEND = {
    "verified": "проверено модератором или носителем",
    "dictionary": "напечатано в словаре",
    "corpus": "встречается в текстах корпуса",
    "rule": "построено по правилам, в текстах не найдено — может быть неверным",
    "auto": "заполнено автоматически, не проверено",
}
WORD_VERDICTS = {   # та же проверка, что у помощника сайта (Оатхал)
    "confirmed": "подтверждено: словарь, словник, таблица карточки, анализатор, корпус, разговорник или "
                 "правки и ответы носителей",
    "unverified": "нигде не подтверждено — вероятно ошибка или выдуманная форма",
}
POS_HINT = {"noun": "NOUN", "verb": "VERB", "adj": "ADJ"}


def _url_of(item: dict[str, Any], fallback: str) -> str:
    path = item.get("path") or (f"/word/{item['entry_id']}" if item.get("entry_id") else "")
    return site_url(path) if path else fallback


def shape_search(raw: dict[str, Any], query: str) -> dict[str, Any]:
    entries = []
    for e in raw.get("entries") or []:
        item = {"id": e.get("id"), "term": e.get("term") or "", "pos": e.get("pos") or "",
                "definition": e.get("definition") or "", "sources": list(e.get("sources") or []),
                "url": site_url(e.get("path") or f"/word/{e.get('id')}")}
        with_report(item)
        for k in ("printed", "noun_class", "usage", "matched_form", "main_forms"):
            if e.get(k):
                item[k] = e[k]
        if e.get("examples"):        # примеры карточки (у первой статьи)
            item["examples"] = [{**x, "url": item["url"]} for x in e["examples"] if isinstance(x, dict)]
        entries.append(item)
    forms = [with_report({"lemma": f.get("lemma") or "", "pos": f.get("pos") or "",
                          "url": _url_of(f, page_url("/razbor", f.get("lemma") or ""))})
             for f in raw.get("wordform_of") or [] if f.get("lemma")]
    ozdoev = []
    for o in raw.get("ozdoev") or []:
        o_url = page_url("/", str(o.get("headword") or "").replace("\u0301", ""))
        senses = [{**s, "examples": [{**x, "url": o_url} for x in s.get("examples") or []]}
                  for s in o.get("senses") or []]
        ozdoev.append({**o, "senses": senses, "url": o_url})
    out: dict[str, Any] = {"found": bool(entries or forms or ozdoev), "query": query, "entries": entries}
    if forms:
        out["wordform_of"] = forms
    if ozdoev:
        out["ozdoev"] = ozdoev
    sugg = [s for s in raw.get("suggestions") or [] if s][:5]
    notes = []
    if raw.get("ru_lemma") and entries:
        notes.append(f"Искали по начальной форме «{raw['ru_lemma']}».")
    if not out["found"]:
        notes.append("В словаре PaydaDosh этого нет." + (f" Похожие написания: {', '.join(sugg)}." if sugg else ""))
        out["suggestions"] = [{"word": s, "url": page_url("/", s)} for s in sugg]
    elif not entries and forms:
        notes.append("Это словоформа, а не заголовок: лемма в wordform_of; полный разбор — analyze_word.")
    if notes:
        out["note"] = " ".join(notes)
    out["url"] = page_url("/", query)
    return out


def _wkey(s: str) -> str:
    return normalize_query(s).lower().replace(PALOCHKA.lower(), "ӏ").replace(PALOCHKA, "ӏ")


def _has_whole_word(text: str, keys: list[str]) -> bool:
    """Слово (форма или лемма) стоит в предложении целым словом, а не куском другого."""
    if not keys:
        return True
    words = re.findall(rf"[{_CYR}A-Za-z]+", _wkey(text))
    return any(k in words for k in keys)


def _segments(main: dict[str, Any]) -> str:
    return "|".join(s.get("text") or "" for s in main.get("segments") or [] if s.get("text"))


def shape_analysis(raw: dict[str, Any], word: str) -> dict[str, Any]:
    main = raw.get("main")
    articles = [with_report({"term": a.get("term") or "", "pos": a.get("pos") or "", "gloss": a.get("gloss") or "",
                             "url": _url_of(a, page_url("/", a.get("term") or ""))})
                for a in (raw.get("articles") or [])[:5]]
    out: dict[str, Any] = {"found": bool(main) or bool(articles), "word": word}
    if main:
        analysis = {
            "lemma": main.get("lemma") or "", "pos": main.get("pos") or "",
            "is_form": bool(main.get("is_form")),
            "description": main.get("desc") or "",
            "confidence": LEVEL.get(main.get("level") or "", "hypothesis"),
            "method": main.get("method") or "",
            "url": _url_of(main, page_url("/razbor", word)),
        }
        with_report(analysis)
        if main.get("also"):
            analysis["also"] = list(main["also"])[:6]
        seg = _segments(main)
        if seg:
            analysis["segments"] = seg
        if main.get("gloss"):
            analysis["gloss"] = main["gloss"]
        out["analysis"] = analysis
        others = [with_report({"lemma": o.get("lemma") or "", "pos": o.get("pos") or "", "note": o.get("note") or "",
                               "gloss": o.get("gloss") or "",
                               "confidence": LEVEL.get(o.get("level") or "", "hypothesis"),
                               "url": _url_of(o, page_url("/razbor", o.get("lemma") or ""))})
                  for o in (raw.get("others") or [])[:6]]
        if others:
            out["other_analyses"] = others
        out["has_paradigm"] = bool(raw.get("paradigm"))
    if articles:
        out["dictionary_entries"] = articles
    ex = raw.get("examples") or {}
    # пример разбора ищется подстрокой («улг» находил «кулг»): оставляем, где форма или лемма — целым словом
    keys = [k for k in {_wkey(word), _wkey((main or {}).get("lemma") or "")} if k]
    rows = [{"ing": _clip(r.get("ing"), EXAMPLE_CHARS), "ru": _clip(r.get("ru"), EXAMPLE_CHARS),
             "source": r.get("code") or "", "page": str(r.get("page") or ""),
             "translation": PROV_RAZBOR.get(r.get("prov") or "", "draft"),
             "url": page_url("/corpus", ex.get("needle") or word)}
            for r in (ex.get("rows") or [])[:3] if _has_whole_word(r.get("ing") or "", keys)]
    if rows:
        out["examples"] = rows
        out["examples_total"] = int(ex.get("total") or len(rows))
    if not out["found"]:
        sugg = [s for s in raw.get("suggestions") or [] if s][:5]
        note = "Разобрать не удалось: такой формы нет ни в словаре, ни у анализатора."
        if sugg:
            note += f" Похожие написания: {', '.join(sugg)}."
            out["suggestions"] = [{"word": s, "url": page_url("/razbor", s)} for s in sugg]
        hyp = raw.get("hypothesis")
        if hyp and hyp.get("lemma"):
            out["unverified_guess"] = {"lemma": hyp["lemma"], "pos": hyp.get("pos") or "",
                                       "note": hyp.get("note") or "",
                                       "warning": "догадка анализатора, не подтверждена словарём"}
        out["note"] = note
    elif main and main.get("level") == "hyp":
        out["note"] = "Разбор — гипотеза анализатора, словарём не подтверждён."
    out["url"] = page_url("/razbor", word)
    return out


MAIN_SLOTS = {   # главные строки — для main_only (у глагола таблица бывает больше сотни строк)
    "verb": {"Inf", "Masd", "Pres", "Aor", "Perf", "Plusq", "Fut", "PossFut", "NegPres", "NegAor", "NegPerf",
             "NegFut", "Imp", "Imp2", "Opt", "Conv", "ConvPst", "Part", "PartPst"},
}


def shape_paradigm(raw: dict[str, Any], word: str, limit: int = MAX_RESULT_BYTES,
                   main_only: bool = False) -> dict[str, Any]:
    para = raw.get("paradigm")
    main = raw.get("main") or {}
    if not para or not para.get("rows"):
        if main:
            note = (f"У «{main.get('lemma') or word}» ({main.get('pos') or '—'}) таблицы форм в PaydaDosh нет: "
                    "слово не изменяется или формы ещё не собраны.")
        else:
            note = "Слово не найдено: таблицы форм нет."
        out = {"found": False, "word": word, "note": note, "url": page_url("/razbor", word)}
        if main.get("lemma"):
            out["lemma"] = main["lemma"]
        return out
    keep = MAIN_SLOTS.get(para.get("kind") or "") if main_only else None
    rows = []
    for r in para["rows"]:
        if keep is not None and (r.get("slot") or "") not in keep:
            continue
        forms = [{k: f[k] for k in ("form", "tier", "label", "hit") if f.get(k)} for f in r.get("forms") or []]
        if not forms:
            continue
        row = {"slot": r.get("slot") or "", "label": r.get("label") or "", "forms": forms}
        for k in ("label_ing", "question"):
            if r.get(k):
                row[k] = r[k]
        if isinstance(r.get("cls"), dict) and r["cls"]:
            row["cls"] = {k: v for k, v in r["cls"].items() if k in "вйбд" and v}
        rows.append(row)
    url = site_url(main["path"]) if main.get("path") else page_url("/razbor", word)
    out: dict[str, Any] = {"found": bool(rows), "word": word, "lemma": para.get("lemma") or "",
                           "part_of_speech": para.get("kind_ru") or para.get("kind") or "",
                           "rows": rows, "tiers": TIER_LEGEND, "url": url}
    with_report(out)
    if isinstance(para.get("noun_class"), dict):
        out["noun_class"] = para["noun_class"]      # класс в ед. и мн. ч. — по статьям семьи, как на карточке
    if any(r.get("cls") for r in rows):
        out["cls_note"] = ("cls — та же форма в других классах {в, й, б, д}: класс по абсолютиву "
                           "(субъект непереходного / объект переходного глагола)")
    if main.get("lemma") and main.get("lemma") != para.get("lemma"):
        out["note"] = f"Таблица леммы «{para.get('lemma')}»."
    # таблица глагола бывает большой: сначала снимаем подписи; классный ряд — последним
    for drop in (("question",), ("label_ing",), ("cls",)):
        if _size(out) <= limit:
            break
        for row in rows:
            for k in drop:
                row.pop(k, None)
        if drop == ("cls",):
            out["note"] = (out.get("note", "") + " Классные ряды не поместились — запроси main_only=true.").strip()
    return out


def shape_spelling(raw: dict[str, Any], text: str, truncated: bool) -> dict[str, Any]:
    checked = int(raw.get("checked_words") or 0)
    issues = [{**i, "url": page_url("/razbor", (i.get("suggestions") or [i.get("word") or ""])[0])}
              for i in raw.get("issues") or []]
    grammar = list(raw.get("grammar") or [])
    errors = int(raw.get("errors") or 0)
    out: dict[str, Any] = {"found": checked > 0, "checked_words": checked, "errors": errors,
                           "all_correct": checked > 0 and errors == 0 and not grammar,
                           "issues": issues, "grammar": grammar}
    if raw.get("phrase"):
        out["phrase_suggestion"] = raw["phrase"].get("text") or ""
    notes = []
    if not checked:
        notes.append("В тексте нет слов кириллицей — проверять нечего.")
    if truncated:
        notes.append(f"Проверены первые {TEXT_MAX} знаков.")
    if issues:
        notes.append("kind: error — нет в словнике; palochka — палочка набрана не той буквой; "
                     "rare — редкая форма, не ошибка; russian — похоже на русское слово.")
    if notes:
        out["note"] = " ".join(notes)
    out["dictionary_version"] = raw.get("dictionary_version") or ""
    out["url"] = PUBLIC_SITE + "/spellcheck"
    return out


def _parts_text(parts: Any) -> str:
    if isinstance(parts, str):
        return parts
    return "".join(str(p[0]) for p in parts or [] if isinstance(p, (list, tuple)) and p)


_OWN_HOSTS = {"paydadosh.ru", "www.paydadosh.ru", "127.0.0.1", "localhost"}
_LETTER = re.compile(rf"[{_CYR}A-Za-z]")


def own_path(url: Any) -> str:
    """Путь страницы НАШЕГО сайта: относительный — как есть, абсолютный — только с нашим хостом (сайт строит
    адрес читалки от своего SITE_URL). Чужой адрес (сайт газеты) — пусто."""
    from urllib.parse import urlsplit

    u = str(url or "").strip()
    if u.startswith("/"):
        return u
    try:
        p = urlsplit(u)
    except ValueError:
        return ""
    if p.scheme in ("http", "https") and (p.hostname or "") in _OWN_HOSTS and p.path.startswith("/"):
        return p.path + (f"?{p.query}" if p.query else "") + (f"#{p.fragment}" if p.fragment else "")
    return ""


def _whole_word_hit(parts: Any) -> bool:
    """Есть ли среди подсвеченных кусков предложения целое слово: поиск корпуса буквальный, и «жим» иначе
    находится внутри «режим», «улг» — внутри «кулг»."""
    if not isinstance(parts, list):
        return True
    for i, p in enumerate(parts):
        if not (isinstance(p, (list, tuple)) and len(p) > 1 and p[1]):
            continue
        before = str(parts[i - 1][0]) if i > 0 else ""
        after = str(parts[i + 1][0]) if i + 1 < len(parts) else ""
        if not (before and _LETTER.match(before[-1])) and not (after and _LETTER.match(after[0])):
            return True
    return False


def shape_examples(raw: dict[str, Any], word: str) -> dict[str, Any]:
    rows, skipped = [], 0
    for it in raw.get("items") or []:
        if len(rows) >= EXAMPLES_MAX:
            break
        if not _whole_word_hit(it.get("parts")):
            skipped += 1
            continue
        ing = _clip(_parts_text(it.get("parts")), EXAMPLE_CHARS)
        if not ing:
            continue
        doc = it.get("doc") if isinstance(it.get("doc"), dict) else None
        path = own_path(doc.get("url")) if doc else ""
        row = {"ing": ing, "ru": _clip(_parts_text(it.get("translation")), EXAMPLE_CHARS),
               "translation": PROV_CORPUS.get(it.get("prov") or "", "none"),
               "source": it.get("code") or "", "work": _clip(it.get("work"), 160),
               "url": site_url(path) if path else page_url("/corpus", word)}
        if it.get("page"):
            row["page"] = str(it["page"])
        if doc and not path and (doc.get("title") or doc.get("url")):
            row["publication"] = {k: doc[k] for k in ("title", "date", "url") if doc.get(k)}
        rows.append(row)
    total = int(raw.get("total") or 0)
    out: dict[str, Any] = {"found": bool(rows), "word": word, "examples": rows, "total": max(total, len(rows))}
    if raw.get("forms"):
        out["matched_forms"] = list(raw["forms"])[:6]
    if not rows:
        out["note"] = ("В корпусе предложений с этим словом нет." if not skipped else
                       "Целым словом в первой странице выдачи не встречается — только внутри других слов.")
    elif total > len(rows):
        out["note"] = (f"Показаны первые {len(rows)}; всего совпадений в корпусе {total} (вместе с совпадениями "
                       "внутри других слов); остальные — на сайте по ссылке url.")
    out["url"] = page_url("/corpus", word)
    return out


def shape_context(raw: dict[str, Any], question: str) -> dict[str, Any]:
    """Контекст перевода: каждый блок — как у помощника сайта; у всего, что имеет страницу, — url."""
    def with_url(items: list[dict[str, Any]], fallback: Callable[[dict[str, Any]], str]) -> list[dict[str, Any]]:
        out = []
        for it in items or []:
            it = dict(it)
            path = it.pop("path", "")
            it["url"] = site_url(path) if path else fallback(it)
            out.append(with_report(it))
        return out

    corpus_url = page_url("/corpus", question)
    out: dict[str, Any] = {"found": bool(raw.get("found")), "question": question,
                           "direction": raw.get("direction") or ""}
    if raw.get("corrections"):
        out["native_corrections"] = list(raw["corrections"])
    if raw.get("ozdoev"):
        out["ozdoev"] = list(raw["ozdoev"])     # русско-ингушский словарь Оздоева (ОЗД80), как у помощника
    out["dictionary"] = with_url(raw.get("dictionary") or [], lambda it: page_url("/", it.get("term") or ""))
    out["parallel"] = with_url(raw.get("parallel") or [], lambda it: corpus_url)
    out["phrasebook"] = with_url(raw.get("phrasebook") or [], lambda it: PUBLIC_SITE + "/phrasebook")
    if raw.get("qa"):
        out["hattarash_answers"] = with_url(raw["qa"], lambda it: PUBLIC_SITE + "/ask")
    if raw.get("grammar_notes"):
        out["grammar_notes"] = list(raw["grammar_notes"])
    if raw.get("morph_notes"):
        out["morphology"] = list(raw["morph_notes"])
    if raw.get("form_attestations"):
        out["form_attestations"] = [{**f, "url": page_url("/corpus", f.get("form") or "")}
                                    for f in raw["form_attestations"]]
    if raw.get("dictionary_examples"):
        out["dictionary_examples"] = [{**e, "url": page_url("/", e.get("entry") or e.get("ing") or "")}
                                      for e in raw["dictionary_examples"]]
    if raw.get("proverbs"):
        out["proverbs"] = list(raw["proverbs"])
    if raw.get("numbers"):
        out["numbers"] = raw["numbers"]
    out["order"] = ("native_corrections → phrasebook → parallel → ozdoev / dictionary → grammar_notes: "
                    "правка носителя и готовая фраза главнее собственной сборки")
    if not out["found"]:
        out["note"] = "Для этого вопроса в PaydaDosh ничего не нашлось — не сочиняй перевод, скажи об этом."
    out["url"] = page_url("/", question)
    return out


def shape_check_text(raw: dict[str, Any], truncated: bool) -> dict[str, Any]:
    words = []
    for w in raw.get("words") or []:
        w = dict(w)
        path = w.pop("path", "")
        # url — карточка слова, только когда она однозначна (card=true, как ссылки в ответе Оатхала);
        # иначе — разбор самой формы на сайте, где видны все чтения
        w["url"] = site_url(path) if path else page_url("/razbor", w.get("word") or "")
        if w.get("card"):
            with_report(w)
        if w.get("suggestions"):
            w["suggestions"] = [{"word": s, "url": page_url("/razbor", s)} for s in w["suggestions"]]
        words.append(w)
    checked = int(raw.get("checked_words") or 0)
    unknown = list(raw.get("unknown_words") or [])
    out: dict[str, Any] = {"found": checked > 0, "checked_words": checked, "all_confirmed": checked > 0 and not unknown,
                           "unknown_words": unknown, "counts": raw.get("counts") or {}, "words": words,
                           "verdicts": WORD_VERDICTS}
    if raw.get("unverified_quotes"):
        out["unverified_quotes"] = list(raw["unverified_quotes"])
    notes = []
    if not checked:
        notes.append("Ингушских слов в тексте не найдено (русские слова не проверяются).")
    if unknown:
        notes.append("Слова из unknown_words нигде не подтверждены — не выдавай их за ингушские: замени "
                     "подтверждённой формой (suggestions, get_paradigm) или скажи, что не уверен.")
    if out.get("unverified_quotes"):
        notes.append("unverified_quotes — цитаты, приписанные источнику, которых нет в данных дословно.")
    notes.append("lemma / pos — первое чтение анализатора, справка; gloss даётся только при однозначной "
                 "карточке (card=true).")
    if truncated:
        notes.append(f"Проверены первые {TEXT_MAX} знаков.")
    if notes:
        out["note"] = " ".join(notes)
    out["url"] = PUBLIC_SITE + "/spellcheck"
    return out


# ── инструменты ────────────────────────────────────────────────────────────────────────────────
def _word_arg(value: str) -> str:
    return normalize_query(value)[:100]


def _empty(word_key: str, value: str, url: str) -> dict[str, Any]:
    return {"found": False, word_key: value, "note": "Пустой запрос.", "url": url}


def _text_arg(text: str) -> tuple[str, bool]:
    t = normalize_query(text)
    return t[:TEXT_MAX], len(t) > TEXT_MAX


def _unknown_log(words_key: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
    # Из проверок текста в журнал идут только незнакомые словарю слова (≤ 30) — сам текст не пишем.
    def extra(result: dict[str, Any]) -> dict[str, Any]:
        if words_key == "issues":
            words = [i.get("word") for i in result.get("issues") or [] if i.get("kind") == "error"]
        else:
            words = list(result.get(words_key) or [])
        return {"unknown": [str(w)[:40] for w in dict.fromkeys(w for w in words if w)][:30]}
    return extra


async def search_dictionary(
    ctx: Context,
    query: Annotated[str, Field(min_length=1, max_length=100, description=(
        "Ingush or Russian word or short phrase, e.g. «дог», «кӀант», «гӀалгӀай», «сердце», «закрыть»."))],
    direction: Annotated[Literal["auto", "ing_to_rus", "rus_to_ing"], Field(description=(
        "auto (default) detects the language; ing_to_rus — the query is Ingush; rus_to_ing — Russian."))] = "auto",
    limit: Annotated[int, Field(ge=1, le=10, description="Max headwords to return (1–10).")] = 5,
) -> ToolOut:
    q = _word_arg(query)

    async def compute() -> dict[str, Any]:
        if not q:
            return _empty("query", q, PUBLIC_SITE)
        raw = await state().api.get("/api/mcp-feed/v1/search", {"q": q, "direction": direction, "limit": limit})
        return shape_search(raw, q)

    return await run_tool("search_dictionary", ctx, {"q": q, "d": direction, "n": limit}, q, compute)


async def analyze_word(
    ctx: Context,
    word: Annotated[str, Field(min_length=1, max_length=100, description=(
        "One Ingush word form, e.g. «дегаш», «аргдий», «цӀагӀа», «бераша»."))],
) -> ToolOut:
    w = _word_arg(word)

    async def compute() -> dict[str, Any]:
        if not w:
            return _empty("word", w, PUBLIC_SITE + "/razbor")
        raw = await state().api.get("/api/mcp-feed/v1/razbor", {"q": w})
        return shape_analysis(raw, w)

    return await run_tool("analyze_word", ctx, {"w": w}, w, compute)


async def get_paradigm(
    ctx: Context,
    word: Annotated[str, Field(min_length=1, max_length=100, description=(
        "Ingush lemma or any of its forms, e.g. «дог», «дегаш», «ала», «кӀант»."))],
    part_of_speech: Annotated[Literal["any", "noun", "verb", "adj"], Field(description=(
        "Pick the table type when the word is both, e.g. «ала» (verb 'to say' / noun 'flame')."))] = "any",
    entry_id: Annotated[int, Field(ge=0, description=(
        "Optional id of a headword from search_dictionary — picks the right homonym."))] = 0,
    main_only: Annotated[bool, Field(description=(
        "Only the principal rows (verbs: infinitive, masdar, present, past, perfect, future, negatives, "
        "imperative, converbs, participles) — a verb table otherwise has 100+ rows."))] = False,
) -> ToolOut:
    w = _word_arg(word)

    async def compute() -> dict[str, Any]:
        if not w:
            return _empty("word", w, PUBLIC_SITE + "/razbor")
        params: dict[str, Any] = {"q": w, "lemma": w, "classes": 1}
        if part_of_speech in POS_HINT:
            params["pos"] = POS_HINT[part_of_speech]
        if entry_id:
            params["entry"] = entry_id
        raw = await state().api.get("/api/mcp-feed/v1/razbor", params)
        return shape_paradigm(raw, w, main_only=main_only)

    return await run_tool("get_paradigm", ctx, {"w": w, "p": part_of_speech, "e": entry_id, "m": main_only},
                          w, compute)


async def find_examples(
    ctx: Context,
    word: Annotated[str, Field(min_length=2, max_length=60, description=(
        "Ingush word (its forms are found too) or a Russian word to look up in translations, "
        "e.g. «дог», «бӀаргаш», «сердце»."))],
) -> ToolOut:
    w = _word_arg(word)

    async def compute() -> dict[str, Any]:
        if len(w) < 2:
            return _empty("word", w, PUBLIC_SITE + "/corpus")
        # только первая страница выдачи: ни `page`, ни `source` не передаём (права изданий)
        raw = await state().api.get("/api/app/v1/corpus", {"q": w})
        return shape_examples(raw, w)

    return await run_tool("find_examples", ctx, {"w": w}, w, compute)


async def translation_context(
    ctx: Context,
    question: Annotated[str, Field(min_length=1, max_length=500, description=(
        "The user's question or phrase as is, e.g. «Как сказать „я хочу домой“?», «Мел ха яьннай хьа?», "
        "«как ты себя чувствуешь»."))],
) -> ToolOut:
    q = normalize_query(question)[:500]

    async def compute() -> dict[str, Any]:
        if not q:
            return _empty("question", q, PUBLIC_SITE)
        raw = await state().api.get("/api/mcp-feed/v1/context", {"q": q}, timeout=CONTEXT_TIMEOUT)
        return shape_context(raw, q)

    return await run_tool("translation_context", ctx, {"q": q}, q, compute, limiter=state().context_limiter)


async def check_ingush_text(
    ctx: Context,
    text: Annotated[str, Field(min_length=1, max_length=5000, description=(
        "Ingush text to verify — typically your own draft answer; Russian words in it are recognised and "
        "skipped. The first 1000 characters are checked. E.g. «Сона хьо веза, хьо са дог ва»."))],
) -> ToolOut:
    t, truncated = _text_arg(text)

    async def compute() -> dict[str, Any]:
        if not t:
            return _empty("text", t, PUBLIC_SITE + "/spellcheck")
        raw = await state().api.post("/api/mcp-feed/v1/check-text", {"text": t})
        return shape_check_text(raw, truncated)

    key = hashlib.sha256(t.encode()).hexdigest()
    return await run_tool("check_ingush_text", ctx, {"t": key}, "", compute,
                          log_extra=lambda r: {"chars": len(t), **_unknown_log("unknown_words")(r)})


async def check_spelling(
    ctx: Context,
    text: Annotated[str, Field(min_length=1, max_length=5000, description=(
        "Ingush text; the first 1000 characters are checked. E.g. «Со гӀалгӀай ва», «Сона хьо веза»."))],
) -> ToolOut:
    t, truncated = _text_arg(text)

    async def compute() -> dict[str, Any]:
        if not t:
            return _empty("text", t, PUBLIC_SITE + "/spellcheck")
        raw = await state().api.post("/api/mcp-feed/v1/spell", {"text": t})
        return shape_spelling(raw, t, truncated)

    key = hashlib.sha256(t.encode()).hexdigest()
    return await run_tool("check_spelling", ctx, {"t": key}, "", compute,
                          log_extra=lambda r: {"chars": len(t), **_unknown_log("issues")(r)})


TOOLS: list[tuple[Callable[..., Any], str, str, str]] = [
    (search_dictionary, "search_dictionary", "Search the Ingush–Russian dictionary", (
        "Look up a word in PaydaDosh, the Ingush–Russian dictionary (headwords merged from dozens of printed "
        "dictionaries). Works both ways: Ingush → Russian («дог», «кӀант», «гӀалгӀай») and Russian → Ingush "
        "(«сердце», «закрыть», «спасибо»; inflected Russian forms are reduced to the dictionary form). "
        "Palochka may be typed as I, 1, l or |. Returns headwords with part of speech, Russian definition, "
        "the codes of the printed sources and a link to the word page; for Russian queries also entries of "
        "Ozdoev's Russian–Ingush dictionary (ОЗД80). If the query is an inflected Ingush form, `wordform_of` "
        "names its lemma — call analyze_word for the full analysis. found=false means the dictionary has no "
        "such word: say so, do not invent one.")),
    (analyze_word, "analyze_word", "Analyze an Ingush word form", (
        "Morphological analysis of one Ingush word form: lemma, part of speech, grammatical description "
        "(case, number, tense, mood…), confidence (high / medium / hypothesis), morpheme segmentation, "
        "dictionary gloss and alternative analyses, plus up to 3 corpus examples with translation. "
        "Examples: «дегаш» (plural of «дог» 'heart'), «аргдий», «цӀагӀа». If the form is unknown, returns "
        "found=false with spelling suggestions — do not invent a form.")),
    (get_paradigm, "get_paradigm", "Get all forms of an Ingush word", (
        "Inflection table of an Ingush word — noun cases and numbers, verb tenses and moods, adjective forms — "
        "as on the word page of paydadosh.ru. `word` is always required: a lemma or any form («дог», «дегаш», "
        "«ала»); entry_id from search_dictionary additionally picks the homonym. Every form has a reliability "
        "tier: verified (checked by a "
        "moderator or native speaker), dictionary (printed in a dictionary), corpus (attested in texts), rule "
        "(generated by rules, not attested — may be wrong), auto (filled automatically, unchecked). Rows of "
        "verbs (and free adjective forms) carry the class row `cls` {в, й, б, д: form} as on the word card — "
        "pick the class of the absolutive; nouns carry `noun_class` with singular and plural class. "
        "main_only=true returns only the principal rows.")),
    (find_examples, "find_examples", "Find example sentences in Ingush texts", (
        "Up to 5 sentences from the PaydaDosh corpus of Ingush texts (books, press, folklore) with the word "
        "or its forms, with Russian translation where available (translation: verified / published / draft "
        "/ none) and the source. First page of results only; the full list is on the site (url). Sentences "
        "are short quotations from copyrighted publications — always cite the source. Examples: «дог», «бӀаргаш».")),
    (translation_context, "translation_context", "Get the translation context for a question", (
        "Everything PaydaDosh knows that is relevant to translating a phrase or answering a question about "
        "Ingush — the same material the site's own assistant reads before answering: native-speaker "
        "corrections approved by the editors (use them verbatim), ready phrasebook phrases (check the "
        "occasion), parallel Ingush–Russian sentences from the corpus, dictionary entries, Ozdoev's "
        "Russian–Ingush dictionary with its phraseology, grammar notes and native speakers' answers from the "
        "Q&A section «Хаттараш». Call it FIRST for any phrase translation "
        "(Russian → Ingush or Ingush → Russian), e.g. «Как сказать „я хочу домой“?», «Мел ха яьннай хьа?». "
        "Heavier than other tools: limited to 20 calls per minute.")),
    (check_ingush_text, "check_ingush_text", "Check Ingush text word by word", (
        "Verify an Ingush text — typically your own draft answer before you send it — with exactly the check "
        "the site's own assistant applies to its answers. Every Ingush word gets a verdict: confirmed (found in "
        "the dictionary, spelling lexicon, a card's form table, a confident analyzer reading, the corpus, the "
        "phrasebook or native speakers' corrections and answers) or unverified (not confirmed anywhere — likely "
        "an error or an invented form, with suggestions). Russian words in the text are skipped. Confirmed words "
        "come with the analyzer's reading (lemma, part of speech, note), and with a gloss and card link only "
        "when the card is unambiguous. Quotes attributed to a source that are not in the data are listed in "
        "unverified_quotes. Replace or flag every unverified word instead of presenting it as Ingush. Example: "
        "«Сона хьо веза, хьо са дог ва». Only the unverified words are kept in our log, not the text.")),
    (check_spelling, "check_spelling", "Spell-check Ingush text", (
        "Spell-check a person's Ingush text (up to 1000 characters) with the PaydaDosh spelling dictionary. "
        "Returns each problematic word with its position, kind (error; palochka typed with a wrong letter; "
        "rare form; Russian word) and suggestions, class-agreement grammar warnings and, for short phrases, a "
        "corrected phrase. Example: «Со гӀалгӀай ва». Only the misspelled words are kept in our log, not the text.")),
]


def ingush_assistant(question: str = "") -> str:
    """Правила ответа об ингушском языке с опорой на инструменты PaydaDosh."""
    return ASSISTANT_RULES + (f"\nВОПРОС ПОЛЬЗОВАТЕЛЯ\n{question.strip()}\n" if question and question.strip() else "")


# ── сервер ─────────────────────────────────────────────────────────────────────────────────────
def _allowed(name: str, default: str) -> list[str]:
    return [x.strip() for x in _env(name, default).split(",") if x.strip()]


ALLOWED_HOSTS = _allowed("MCP_ALLOWED_HOSTS", "paydadosh.ru,www.paydadosh.ru,127.0.0.1,127.0.0.1:*,localhost,localhost:*")


def host_allowed(host: str) -> bool:
    """Та же проверка Host, что у библиотеки (точное имя или «имя:*» для любого порта)."""
    host = (host or "").strip().lower()
    for pat in ALLOWED_HOSTS:
        pat = pat.lower()
        if host == pat or (pat.endswith(":*") and host.startswith(pat[:-1])):
            return True
    return False


def create_mcp() -> FastMCP:
    mcp = FastMCP(
        name="paydadosh",
        instructions=INSTRUCTIONS,
        website_url=PUBLIC_SITE + "/developers",
        icons=[Icon(src=PUBLIC_SITE + "/static/apple-touch-icon.png", mimeType="image/png", sizes=["180x180"])],
        host=HOST,
        port=PORT,
        streamable_http_path="/mcp",
        stateless_http=True,
        json_response=True,
        max_request_body_size=MAX_BODY_BYTES,
        log_level="WARNING",
        # Host проверяем (прокси передаёт Host сайта); Origin есть только у браузеров — им разрешены сайт и
        # локальный инспектор. Облачные клиенты Origin не шлют.
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=ALLOWED_HOSTS,
            allowed_origins=_allowed("MCP_ALLOWED_ORIGINS",
                                     "https://paydadosh.ru,http://localhost:*,http://127.0.0.1:*"),
        ),
    )
    mcp._mcp_server.version = VERSION
    for fn, name, title, description in TOOLS:
        mcp.add_tool(fn, name=name, title=title, description=description,
                     annotations=ToolAnnotations(title=title, **_READ_ONLY))
    mcp.add_prompt(Prompt.from_function(
        ingush_assistant, name="ingush_assistant", title="Ingush assistant / Ингушский помощник",
        description=("Rules for answering questions about the Ingush language with the PaydaDosh tools: "
                     "which tool first, never invent forms, cite sources, link every word to paydadosh.ru, "
                     "and the most common grammar traps.")))

    @mcp.custom_route("/healthz", methods=["GET"])
    async def healthz(request: Request) -> Response:
        return JSONResponse({"ok": True, "version": VERSION})

    return mcp


class Gateway:
    """Обёртка над приложением MCP: (1) из `initialize` запоминает `clientInfo.name` и пишет строку
    «подключение» в журнал; (2) браузер, открывший /mcp, уходит на страницу для разработчиков; (3) тело больше
    64 КБ не читаем."""

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers") or []}
        method = scope.get("method")
        accept = headers.get("accept", "")
        if method == "GET" and "text/html" in accept and "text/event-stream" not in accept:
            await RedirectResponse(PUBLIC_SITE + "/developers", status_code=302)(scope, receive, send)
            return
        if method != "POST":
            await self.app(scope, receive, send)
            return
        try:
            declared = int(headers.get("content-length") or 0)
        except ValueError:
            declared = 0
        if declared > MAX_BODY_BYTES:
            await Response("Request body too large", status_code=413)(scope, receive, send)
            return
        chunks: list[bytes] = []
        size, more = 0, True
        while more:
            message = await receive()
            if message["type"] != "http.request":
                return  # клиент ушёл
            chunk = message.get("body", b"")
            size += len(chunk)
            if size > MAX_BODY_BYTES:
                await Response("Request body too large", status_code=413)(scope, receive, send)
                return
            chunks.append(chunk)
            more = message.get("more_body", False)
        body = b"".join(chunks)
        try:
            self._sniff(body, headers, scope)
        except Exception:
            log.debug("initialize sniff failed", exc_info=True)
        replayed = False

        async def replay() -> dict[str, Any]:
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        await self.app(scope, replay, send)

    @staticmethod
    def _sniff(body: bytes, headers: dict[str, str], scope: dict[str, Any]) -> None:
        if b'"initialize"' not in body or not host_allowed(headers.get("host", "")):
            return  # чужой Host библиотека всё равно отклонит — в журнал и память не пишем
        msg = json.loads(body)
        for m in msg if isinstance(msg, list) else [msg]:
            if not isinstance(m, dict) or m.get("method") != "initialize":
                continue
            params = m.get("params") or {}
            info = params.get("clientInfo") or {}
            client = (scope.get("client") or ("", 0))[0]
            ip, ua = _headers_ip_ua(headers, client)
            st = state()
            iph = st.journal.ip_hash(ip)
            name = client_name(info.get("name"))
            st.clients.remember((iph, ua), name)
            st.journal.write({"tool": "initialize", "q": "", "found": None, "ms": 0,
                              "client": name or ua_family(ua), "client_version": client_name(info.get("version"))[:20],
                              "protocol": str(params.get("protocolVersion") or "")[:12], "ip": iph, "cached": False})


def create_app() -> Gateway:
    return Gateway(create_mcp().streamable_http_app())


app = create_app()


def main() -> None:
    import uvicorn

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    uvicorn.run(app, host=HOST, port=PORT, log_level="info", proxy_headers=False, server_header=False,
                access_log=False)


if __name__ == "__main__":
    main()
