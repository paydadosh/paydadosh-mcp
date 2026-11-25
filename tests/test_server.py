"""MCP-сервер PaydaDosh (`server.py`): ответы инструментов и протокол Streamable HTTP.

Сайт подменён: его внутренний HTTP отвечает записанными ответами (`tests/fixtures/*.json`, сняты с локальной
копии сайта 22.10.2026). Запуск из каталога службы: `python -m pytest`.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date
from pathlib import Path

import pytest

pytest.importorskip("mcp")
import httpx  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

HERE = Path(__file__).resolve().parents[1]
FIX = HERE / "tests" / "fixtures"
P = "Ӏ"


def _load_server():
    spec = importlib.util.spec_from_file_location("paydadosh_mcp_server", HERE / "server.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


srv = _load_server()


def fixture(name: str) -> dict:
    return json.loads((FIX / f"{name}.json").read_text(encoding="utf-8"))


ROUTES = {
    ("/api/mcp-feed/v1/search", f"к{P}ант"): "search_kant",
    ("/api/mcp-feed/v1/search", "дог"): "search_dog",
    ("/api/mcp-feed/v1/search", "сердце"): "search_heart",
    ("/api/mcp-feed/v1/search", "дегаш"): "search_degash",
    ("/api/mcp-feed/v1/razbor", "дегаш"): "razbor_degash",
    ("/api/mcp-feed/v1/razbor", "ала"): "paradigm_ala_verb",
    ("/api/mcp-feed/v1/razbor", "дог"): "paradigm_dog",
    ("/api/app/v1/corpus", "дог"): "corpus_dog",
    ("/api/mcp-feed/v1/context", "Как ты себя чувствуешь?"): "context_feel",
}
MISS = {"/api/mcp-feed/v1/search": "search_miss", "/api/mcp-feed/v1/razbor": "razbor_miss",
        "/api/app/v1/corpus": "corpus_miss", "/api/mcp-feed/v1/spell": "spell",
        "/api/mcp-feed/v1/context": "context_home", "/api/mcp-feed/v1/check-text": "check_text"}


class FakeSite:
    """Внутренний HTTP сайта: записанные ответы; запоминает, что спросили."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.timeouts: list[float] = []
        self.fail: str | None = None

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        params = dict(request.url.params)
        if request.method == "POST":
            params = json.loads(request.content)
        self.calls.append((path, params))
        self.timeouts.append(request.extensions.get("timeout", {}).get("read") or 0)
        if self.fail == "timeout":
            raise httpx.ReadTimeout("slow", request=request)
        if self.fail == "500":
            return httpx.Response(500, text="boom")
        name = ROUTES.get((path, params.get("q"))) or MISS.get(path)
        if name is None:
            return httpx.Response(404)
        return httpx.Response(200, json=fixture(name))


@pytest.fixture()
def site(tmp_path):
    fake = FakeSite()
    srv.STATE = srv.State(api=srv.SiteAPI(transport=httpx.MockTransport(fake), feed_key=""),
                          journal=srv.Journal(tmp_path, salt="test-salt"))
    yield fake
    srv.STATE = None


def journal(tmp_path) -> list[dict]:
    out = []
    for path in sorted(tmp_path.glob("mcp_calls-*.jsonl")):
        out += [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines()]
    return out


HEADERS = {"Accept": "application/json, text/event-stream", "User-Agent": "pytest-ua/1.0",
           "X-Real-IP": "203.0.113.7", "MCP-Protocol-Version": "2025-06-18"}


@pytest.fixture()
def client(site):
    with TestClient(srv.create_app(), base_url="http://127.0.0.1:8010") as c:
        yield c


def rpc(c, method, params=None, rid=1, headers=None):
    body = {"jsonrpc": "2.0", "id": rid, "method": method, "params": params or {}}
    return c.post("/mcp", json=body, headers={**HEADERS, **(headers or {})})


def call(c, tool, args, rid=2, headers=None):
    r = rpc(c, "tools/call", {"name": tool, "arguments": args}, rid, headers)
    assert r.status_code == 200, r.text
    return r.json()["result"]


def init(c, name="pytest-client"):
    return rpc(c, "initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                 "clientInfo": {"name": name, "version": "9.9"}}, 1)


# ── вход и вид ответа ──────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("raw,want", [
    ("кIант", f"к{P}ант"), ("к1ант", f"к{P}ант"), ("кlант", f"к{P}ант"), ("к|ант", f"к{P}ант"),
    ("кӏант", f"к{P}ант"), ("гIалгIай", f"г{P}алг{P}ай"), ("Iовдал", f"{P}овдал"),
    ("1941 шу", "1941 шу"), ("Jack London", "Jack London"), ("  дог\u200b  ", "дог"),
])
def test_palochka_input(raw, want):
    assert srv.normalize_query(raw) == want


def test_display_palochka_but_not_urls():
    out = srv.display({"lemma": "кӏант", "rows": [{"form": "дӏа"}], "url": "https://x/word/1-кӏант",
                       "path": "/word/1-кӏант"})
    assert out["lemma"] == f"к{P}ант" and out["rows"][0]["form"] == f"д{P}а"
    assert out["url"].endswith("кӏант") and out["path"].endswith("кӏант")


def test_fit_keeps_answer_small():
    big = {"found": True, "items": [{"text": "д" * 300} for _ in range(200)]}
    out = srv.fit(big, 20_000)
    assert srv._size(out) <= 20_000 and out["truncated"] is True and 0 < len(out["items"]) < 200
    assert "truncated" not in srv.fit({"found": True}, 20_000)


def _urls_everywhere(obj, path="$"):
    """Пути к элементам списков-выдачи без url (у каждого элемента выдачи должна быть ссылка)."""
    missing = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in ("entries", "wordform_of", "ozdoev", "other_analyses", "dictionary_entries", "examples",
                     "issues", "words", "dictionary", "parallel", "phrasebook", "hattarash_answers",
                     "form_attestations", "dictionary_examples", "suggestions") and isinstance(v, list):
                missing += [f"{path}.{k}[{i}]" for i, it in enumerate(v) if isinstance(it, dict) and not it.get("url")]
            missing += _urls_everywhere(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, it in enumerate(obj):
            missing += _urls_everywhere(it, f"{path}[{i}]")
    return missing


# ── ответы инструментов ────────────────────────────────────────────────────────────────────────
def test_search_found_with_sources_and_link():
    out = srv.shape_search(fixture("search_dog"), "дог")
    assert out["found"] is True
    first = out["entries"][0]
    assert first["term"] == "дог" and "сердце" in first["definition"]
    assert "БДИМТ09" in first["sources"] and "PaydaDosh" not in first["sources"]
    assert first["url"].startswith("https://paydadosh.ru/word/104869-") and first["url"].isascii()
    assert out["url"] == "https://paydadosh.ru/?q=%D0%B4%D0%BE%D0%B3"
    assert _urls_everywhere(out) == []


def test_search_not_found_says_so():
    out = srv.shape_search(fixture("search_miss"), "хьозазаза")
    assert out["found"] is False and out["entries"] == [] and "нет" in out["note"]


def test_search_wordform_and_ozdoev_have_links():
    out = srv.shape_search(fixture("search_degash"), "дегаш")
    assert out["wordform_of"][0]["lemma"] == "дог"
    heart = srv.shape_search(fixture("search_heart"), "сердце")
    assert heart["ozdoev"][0]["source"] == "ОЗД80" and heart["ozdoev"][0]["url"].startswith("https://paydadosh.ru/?q=")
    assert _urls_everywhere(out) == [] and _urls_everywhere(heart) == []


def test_no_moderator_names_leak():
    raw = fixture("search_dog")
    raw["entries"][0]["created_by_name"] = "@moder_name"
    raw["entries"][0]["created_by"] = 777
    assert "moder_name" not in json.dumps(srv.shape_search(raw, "дог"), ensure_ascii=False)
    para = fixture("paradigm_dog")
    para["paradigm"]["rows"][0]["forms"][0]["by"] = "@moder_name"
    assert "moder_name" not in json.dumps(srv.shape_paradigm(para, "дог"), ensure_ascii=False)
    for name in ("context_home", "context_feel"):
        text = json.dumps(fixture(name), ensure_ascii=False)
        assert "перевод модератора" not in text and "@" not in text


def test_analysis_form_of_lemma():
    out = srv.shape_analysis(fixture("razbor_degash"), "дегаш")
    assert out["found"] is True
    assert out["analysis"]["lemma"] == "дог" and out["analysis"]["is_form"] is True
    assert out["analysis"]["confidence"] == "high"
    assert out["url"] == "https://paydadosh.ru/razbor?q=%D0%B4%D0%B5%D0%B3%D0%B0%D1%88"
    assert len(out.get("examples") or []) <= 3
    assert _urls_everywhere(out) == []


def test_analysis_unknown_form():
    out = srv.shape_analysis(fixture("razbor_miss"), "хьозазаза")
    assert out["found"] is False and out["suggestions"] and "не удалось" in out["note"]


def test_paradigm_tiers_and_size():
    out = srv.fit(srv.display(srv.shape_paradigm(fixture("paradigm_ala_verb"), "ала")))
    assert out["found"] is True and out["lemma"] == "ала" and out["part_of_speech"] == "глагол"
    tiers = {f["tier"] for r in out["rows"] for f in r["forms"]}
    assert tiers <= set(srv.TIER_LEGEND) | {"other"} and "verified" in tiers
    assert srv._size(out) <= srv.MAX_RESULT_BYTES and "truncated" not in out   # вся таблица глагола влезает


def test_paradigm_missing():
    out = srv.shape_paradigm({"main": {"lemma": "цӀагӀа", "pos": "нареч."}, "paradigm": None}, "цӀагӀа")
    assert out["found"] is False and "таблицы форм" in out["note"]


def test_examples_at_most_five_and_short():
    out = srv.shape_examples(fixture("corpus_dog"), "дог")
    assert out["found"] is True and 1 <= len(out["examples"]) <= 5
    assert all(len(e["ing"]) <= 300 and len(e["ru"]) <= 300 for e in out["examples"])
    assert out["total"] >= len(out["examples"]) and out["url"].startswith("https://paydadosh.ru/corpus?q=")
    assert srv.shape_examples(fixture("corpus_miss"), "хьозазаза")["found"] is False
    assert _urls_everywhere(out) == []


def test_examples_whole_words_and_own_links():
    raw = {"total": 3, "items": [
        {"parts": [["ре", 0], ["жим", 1], [" тяжёлый", 0]], "translation": [], "prov": "", "code": ""},
        {"parts": [["Хьа ", 0], ["жим", 1], [" ва.", 0]], "translation": [["Ты маленький.", 0]], "prov": "published",
         "code": "", "work": "Книга", "doc": {"kind": "doc", "url": "http://127.0.0.1:8000/books/read/7#p3"}},
        {"parts": [["Газета: ", 0], ["жим", 1]], "translation": [], "prov": "", "code": "",
         "doc": {"kind": "press", "url": "https://serdalo.ru/n/1", "title": "Сердало"}},
    ]}
    out = srv.shape_examples(raw, "жим")
    assert [e["ing"] for e in out["examples"]] == ["Хьа жим ва.", "Газета: жим"]   # «режим» отсеян
    assert out["examples"][0]["url"] == "https://paydadosh.ru/books/read/7#p3"      # не 127.0.0.1
    assert "publication" not in out["examples"][0]
    assert out["examples"][1]["publication"]["url"] == "https://serdalo.ru/n/1"
    assert "127.0.0.1" not in json.dumps(out)
    only_inside = srv.shape_examples({"total": 1, "items": raw["items"][:1]}, "жим")
    assert only_inside["found"] is False and "внутри других слов" in only_inside["note"]


def test_spelling_shape():
    out = srv.shape_spelling(fixture("spell"), "текст", truncated=True)
    assert out["found"] is True and out["errors"] >= 1 and out["all_correct"] is False
    assert {i["kind"] for i in out["issues"]} <= {"error", "palochka", "rare", "russian"}
    assert "первые 1000" in out["note"] and _urls_everywhere(out) == []


def test_context_shape():
    out = srv.fit(srv.display(srv.shape_context(fixture("context_feel"), "Как ты себя чувствуешь?")))
    assert out["found"] is True and out["native_corrections"]
    assert len(out["dictionary"]) <= 6 and len(out["parallel"]) <= 3 and len(out["phrasebook"]) <= 3
    assert all(len(p["ing"]) <= 300 for p in out["parallel"])
    assert sum(len(n) for n in out.get("grammar_notes") or []) <= 3000
    assert len(out.get("hattarash_answers") or []) <= 2 and len(out["native_corrections"]) <= 2
    assert srv._size(out) <= srv.MAX_RESULT_BYTES and _urls_everywhere(out) == []


def test_check_text_shape():
    """Вердикт — проверка Оатхала (записанный ответ сайта): unverified / confirmed по каждому слову."""
    out = srv.shape_check_text(fixture("check_text"), truncated=False)
    assert out["found"] is True and out["all_confirmed"] is False and out["unknown_words"] == ["хьозазаза"]
    by = {w["word"]: w for w in out["words"]}
    assert by["дог"]["verdict"] == "confirmed" and by["дог"]["lemma"] == "дог"
    assert by["хьозазаза"]["verdict"] == "unverified" and by["хьозазаза"]["suggestions"]
    assert set(out["verdicts"]) == {"confirmed", "unverified"}
    # ссылка на карточку — только однозначная (card=true), иначе — разбор формы на сайте
    for w in out["words"]:
        if w.get("card"):
            assert w["url"].startswith("https://paydadosh.ru/word/")
        else:
            assert w["url"].startswith("https://paydadosh.ru/razbor?q=") and "gloss" not in w
    assert _urls_everywhere(out) == []


def test_paradigm_class_rows_and_noun_class():
    verb = srv.shape_paradigm(fixture("paradigm_ala_verb"), "ала")
    rows = {r["slot"]: r for r in verb["rows"]}
    assert rows["Fut"]["cls"] == {"в": "аргва", "й": "аргья", "б": "аргба", "д": "аргда"}
    assert "cls_note" in verb and srv._size(srv.fit(srv.display(verb))) <= srv.MAX_RESULT_BYTES
    # вся таблица глагола с классными рядами укладывается: подписи снимаются, ряды остаются
    fitted = srv.fit(srv.display(verb))
    assert "truncated" not in fitted and len(fitted["rows"]) == len(verb["rows"])
    assert sum(1 for r in fitted["rows"] if r.get("cls")) == sum(1 for r in verb["rows"] if r.get("cls"))
    short = srv.shape_paradigm(fixture("paradigm_ala_verb"), "ала", main_only=True)
    assert {r["slot"] for r in short["rows"]} <= srv.MAIN_SLOTS["verb"]
    assert 0 < len(short["rows"]) < len(verb["rows"]) and "question" in short["rows"][0]
    noun = srv.shape_paradigm(fixture("paradigm_dog"), "дог")
    assert noun["noun_class"] == {"raw": "д, д", "singular": ["д"], "plural": "д"}


def _card_items(obj, key=""):
    """(путь, элемент) всех элементов выдачи, чей url — карточка слова; примеры карточки — часть статьи."""
    found = []
    if isinstance(obj, dict):
        if key != "examples" and str(obj.get("url") or "").startswith("https://paydadosh.ru/word/"):
            found.append(obj)
        for k, v in obj.items():
            found += _card_items(v, k)
    elif isinstance(obj, list):
        for x in obj:
            found += _card_items(x, key)
    return found


def test_report_url_on_every_card():
    """Обратная связь — ссылкой: у всего, что ведёт на карточку слова, — форма ошибки на этой карточке."""
    outs = {
        "search": srv.shape_search(fixture("search_dog"), "дог"),
        "wordform": srv.shape_search(fixture("search_degash"), "дегаш"),
        "analysis": srv.shape_analysis(fixture("razbor_degash"), "дегаш"),
        "paradigm": srv.shape_paradigm(fixture("paradigm_dog"), "дог"),
        "check": srv.shape_check_text(fixture("check_text"), truncated=False),
        "context": srv.shape_context(fixture("context_feel"), "Как ты себя чувствуешь?"),
    }
    for name, out in outs.items():
        items = _card_items(srv.fit(srv.display(out)))
        assert items, name
        for it in items:
            assert it.get("report_url") == it["url"] + "#word-report-error", (name, it.get("url"))
            assert it["report_url"].isascii()
    # не карточка — нет и формы: разбор формы, поиск, слово без однозначной карточки
    assert "report_url" not in srv.shape_search(fixture("search_miss"), "хьозазаза")
    check = outs["check"]
    assert all("report_url" not in w for w in check["words"] if not w.get("card"))
    assert any("report_url" in w for w in check["words"] if w.get("card"))
    assert srv.report_url("https://paydadosh.ru/razbor?q=x") == ""
    assert srv.report_url("https://paydadosh.ru/word/1-x#forms") == "https://paydadosh.ru/word/1-x#word-report-error"


def test_journal_daily_files_and_retention(tmp_path):
    j = srv.Journal(tmp_path, salt="s", retention_days=90)
    old = tmp_path / "mcp_calls-2026-01-01.jsonl"
    keep = tmp_path / "mcp_calls-2026-10-01.jsonl"
    other = tmp_path / "notes.txt"
    for f in (old, keep, other):
        f.write_text("x\n", encoding="utf-8")
    gone = j.purge(date(2026, 10, 22))
    assert gone == [old] and keep.exists() and other.exists()
    j.write({"tool": "t", "q": "дог"})
    assert len(list(tmp_path.glob("mcp_calls-*.jsonl"))) == 2


def test_rate_limiter():
    lim = srv.RateLimiter(3)
    assert [lim.allow("a") for _ in range(4)] == [True, True, True, False]
    assert lim.allow("b") is True


def test_prompt_rules():
    text = srv.ingush_assistant("Как сказать «спасибо»?")
    assert "translation_context" in text and "check_ingush_text" in text and "paydadosh.ru" in text
    assert text.rstrip().endswith("Как сказать «спасибо»?")
    assert "url" in srv.INSTRUCTIONS and "check_ingush_text" in srv.INSTRUCTIONS
    # неверная форма → ссылка на форму ошибки, сам помощник ничего не отправляет
    for rules in (srv.INSTRUCTIONS, text):
        assert "report_url" in rules and "сообщить об ошибке на paydadosh.ru" in rules
        assert "#feedback-form" in rules
    assert "Never report anything yourself" in srv.INSTRUCTIONS and "ничего не отправляй" in text


# ── протокол целиком ───────────────────────────────────────────────────────────────────────────
def test_initialize_list_and_prompt(client, site, tmp_path):
    r = init(client)
    assert r.status_code == 200, r.text
    res = r.json()["result"]
    assert res["serverInfo"]["name"] == "paydadosh"
    assert "found=false" in res["instructions"]
    tools = rpc(client, "tools/list", {}, 2).json()["result"]["tools"]
    assert {t["name"] for t in tools} == {"search_dictionary", "analyze_word", "get_paradigm", "find_examples",
                                          "translation_context", "check_ingush_text", "check_spelling"}
    for t in tools:
        a = t["annotations"]
        assert a["title"] and a["readOnlyHint"] is True and a["destructiveHint"] is False
        assert a["openWorldHint"] is False
    prompts = rpc(client, "prompts/list", {}, 3).json()["result"]["prompts"]
    assert prompts[0]["name"] == "ingush_assistant" and "Ингушский помощник" in prompts[0]["title"]
    got = rpc(client, "prompts/get", {"name": "ingush_assistant", "arguments": {"question": "Как будет хлеб?"}}, 4)
    msg = got.json()["result"]["messages"][0]["content"]["text"]
    assert "Как будет хлеб?" in msg and "ЛОВУШКИ" in msg
    rows = journal(tmp_path)
    assert rows[0]["tool"] == "initialize" and rows[0]["client"] == "pytest-client"
    assert rows[0]["ip"] and "203.0.113.7" not in json.dumps(rows)


def test_every_tool_end_to_end(client, site, tmp_path):
    init(client)
    res = call(client, "search_dictionary", {"query": "кIант"})          # палочка латинской I
    assert res["isError"] is False and res["structuredContent"]["found"] is True
    assert site.calls[-1] == ("/api/mcp-feed/v1/search", {"q": f"к{P}ант", "direction": "auto", "limit": "5"})
    text = json.loads(res["content"][0]["text"])
    assert text["entries"][0]["term"] == f"к{P}ант"
    # строка источника и ссылка-ресурс на страницу сайта
    assert res["content"][1]["text"].startswith("Источник: PaydaDosh — https://paydadosh.ru/")
    assert res["content"][2]["type"] == "resource_link" and res["content"][2]["uri"].startswith("https://paydadosh.ru/")

    assert call(client, "analyze_word", {"word": "дегаш"})["structuredContent"]["analysis"]["lemma"] == "дог"
    res = call(client, "get_paradigm", {"word": "ала", "part_of_speech": "verb", "entry_id": 126600})
    assert res["structuredContent"]["found"] is True
    assert site.calls[-1][1] == {"q": "ала", "lemma": "ала", "pos": "VERB", "entry": "126600", "classes": "1"}
    res = call(client, "find_examples", {"word": "дог"})
    assert len(res["structuredContent"]["examples"]) <= 5
    assert site.calls[-1] == ("/api/app/v1/corpus", {"q": "дог"})        # ни page, ни source
    res = call(client, "translation_context", {"question": "Как ты себя чувствуешь?"})
    assert res["structuredContent"]["native_corrections"]
    assert site.timeouts[-1] == srv.CONTEXT_TIMEOUT                      # у контекста свой таймаут
    res = call(client, "check_ingush_text", {"text": "Сона хьо веза, хьо са дог да. Со тахан цӀагӀа хургвац, хьозазаза."})
    assert res["structuredContent"]["unknown_words"] == ["хьозазаза"]
    res = call(client, "check_spelling", {"text": "Со гӀалгӀай ва гулакх"})
    assert res["structuredContent"]["found"] is True

    rows = [r for r in journal(tmp_path) if r["tool"] != "initialize"]
    assert [r["tool"] for r in rows] == ["search_dictionary", "analyze_word", "get_paradigm", "find_examples",
                                         "translation_context", "check_ingush_text", "check_spelling"]
    assert all(r["client"] == "pytest-client" and r["ip"] and r["found"] is True for r in rows)
    by = {r["tool"]: r for r in rows}
    assert by["translation_context"]["q"] == "Как ты себя чувствуешь?"
    # из проверок текста — только незнакомые слова и длина, текста нет
    assert by["check_ingush_text"]["q"] == "" and by["check_ingush_text"]["unknown"] == ["хьозазаза"]
    assert by["check_ingush_text"]["chars"] > 0 and "Сона" not in json.dumps(by["check_ingush_text"], ensure_ascii=False)
    assert by["check_spelling"]["q"] == "" and by["check_spelling"]["unknown"]


def test_old_protocol_gets_no_resource_link(client, site):
    res = call(client, "search_dictionary", {"query": "дог"}, headers={"MCP-Protocol-Version": "2025-03-26"})
    assert [c["type"] for c in res["content"]] == ["text", "text"]


def test_long_query_logged_up_to_300(client, site, tmp_path):
    q = "как сказать " + "очень " * 80
    call(client, "translation_context", {"question": q[:500]})
    row = journal(tmp_path)[-1]
    assert len(row["q"]) == 300
    call(client, "analyze_word", {"word": "д" * 90}, rid=3)
    assert len(journal(tmp_path)[-1]["q"]) == 64


def test_context_rate_limit(client, site, tmp_path):
    srv.state().context_limiter = srv.RateLimiter(2)
    assert call(client, "translation_context", {"question": "один"})["isError"] is False
    assert call(client, "translation_context", {"question": "два"}, rid=3)["isError"] is False
    res = call(client, "translation_context", {"question": "три"}, rid=4)
    assert res["isError"] is True and "Rate limit" in res["content"][0]["text"]
    assert journal(tmp_path)[-1]["error"] == "rate_limited"
    # уже посчитанный вопрос из кэша лимит не тратит
    assert call(client, "translation_context", {"question": "один"}, rid=5)["isError"] is False
    # другие инструменты не ограничены этим лимитом
    assert call(client, "search_dictionary", {"query": "дог"}, rid=6)["isError"] is False


def test_not_found_and_empty(client, site):
    res = call(client, "search_dictionary", {"query": "хьозазаза"})
    assert res["isError"] is False and res["structuredContent"]["found"] is False
    res = call(client, "analyze_word", {"word": "   "})
    assert res["structuredContent"]["found"] is False and res["structuredContent"]["note"] == "Пустой запрос."


def test_too_long_input_rejected(client, site):
    res = call(client, "analyze_word", {"word": "д" * 101})
    assert res["isError"] is True
    assert site.calls == []


def test_cache_repeats(client, site, tmp_path):
    call(client, "search_dictionary", {"query": "дог"})
    call(client, "search_dictionary", {"query": "дог"}, rid=3)
    assert len(site.calls) == 1
    assert [r["cached"] for r in journal(tmp_path)] == [False, True]


def test_site_down_is_error_not_miss(client, site, tmp_path):
    site.fail = "timeout"
    res = call(client, "search_dictionary", {"query": "дог"})
    assert res["isError"] is True and "not a 'not found'" in res["content"][0]["text"]
    assert journal(tmp_path)[-1]["error"] == "timeout"
    site.fail = None
    res = call(client, "search_dictionary", {"query": "дог"}, rid=3)        # ошибка не закэширована
    assert res["isError"] is False and res["structuredContent"]["found"] is True


def test_hundred_calls_stateless(client, site, tmp_path):
    """100 вызовов подряд без инициализации сессии — сервер без состояния."""
    tools = [("search_dictionary", "query"), ("analyze_word", "word"), ("get_paradigm", "word"),
             ("find_examples", "word")]
    for i in range(100):
        tool, arg = tools[i % len(tools)]
        res = call(client, tool, {arg: f"слово{i}"}, rid=i + 10)
        assert res["isError"] is False, res
    assert len([r for r in journal(tmp_path) if "error" not in r]) == 100


def test_gateway_browser_size_host(client, site):
    r = client.get("/mcp", headers={"Accept": "text/html"}, follow_redirects=False)
    assert r.status_code == 302 and r.headers["location"] == "https://paydadosh.ru/developers"
    r = client.post("/mcp", content=b"{" + b" " * (70 * 1024) + b"}",
                    headers={**HEADERS, "Content-Type": "application/json"})
    assert r.status_code == 413
    assert rpc(client, "tools/list", {}, headers={"Host": "evil.example"}).status_code == 421
    assert rpc(client, "tools/list", {}, headers={"Host": "paydadosh.ru"}).status_code == 200
    assert client.get("/healthz").json()["ok"] is True


# ── ревью безопасности 24.11.2026 (служба может слушать сеть, не только 127.0.0.1) ─────────
def test_real_ip_trusted_only_from_proxy(monkeypatch):
    monkeypatch.setattr(srv, "TRUSTED_PROXIES", {"10.0.0.15"})
    h = {"x-real-ip": "203.0.113.7", "x-forwarded-for": "198.51.100.1", "user-agent": "ua"}
    assert srv._headers_ip_ua(h, "10.0.0.15")[0] == "203.0.113.7"
    # чужой узел сети подставить адрес не может, X-Forwarded-For не читается вовсе
    assert srv._headers_ip_ua(h, "10.0.0.50")[0] == "10.0.0.50"
    assert srv._headers_ip_ua({"x-forwarded-for": "198.51.100.1"}, "10.0.0.15")[0] == "10.0.0.15"


def test_rate_limiter_bounded_and_shared_egress():
    lim = srv.RateLimiter(1, shared_per_minute=3)
    lim.MAX_KEYS = 50
    for i in range(500):
        lim.allow(f"k{i}")
    assert len(lim._hits) == 50
    assert [lim.allow("cloud", shared=True) for _ in range(4)] == [True, True, True, False]
    assert srv.is_shared_egress("160.79.105.1") and not srv.is_shared_egress("203.0.113.7")
    assert not srv.is_shared_egress("not-an-ip")


def test_clients_lru_bounded():
    c = srv.Clients()
    c.MAX_KEYS = 100
    for i in range(1000):
        c.remember(("h", f"ua-{i}" * 50), "claude")
    assert len(c._d) == 100
    assert c.lookup(("h", "ua-999" * 50)) == "claude" and c.lookup(("h", "ua-0" * 50)) == ""


def test_journal_daily_cap(tmp_path):
    j = srv.Journal(tmp_path, salt="s")
    j.max_bytes_per_day = 400
    for i in range(50):
        j.write({"tool": "search_dictionary", "q": "д" * 20})
    rows = [json.loads(x) for f in tmp_path.glob("mcp_calls-*.jsonl") for x in f.read_text(encoding="utf-8").splitlines()]
    assert rows[-1]["tool"] == "journal_capped" and sum(r["tool"] == "journal_capped" for r in rows) == 1
    assert sum(f.stat().st_size for f in tmp_path.glob("*.jsonl")) < 600


def test_sniff_ignores_foreign_host(site, tmp_path):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                       "params": {"clientInfo": {"name": "evil"}}}).encode()
    srv.Gateway._sniff(body, {"host": "evil.example", "user-agent": "x"}, {"client": ("10.0.0.1", 1)})
    assert journal(tmp_path) == []
    srv.Gateway._sniff(body, {"host": "127.0.0.1:8010", "user-agent": "x"}, {"client": ("127.0.0.1", 1)})
    assert journal(tmp_path)[-1]["client"] == "evil"


def test_single_source_rule():
    # 24.11.2026: «амасти» — «фея» только в БДИМТ09; помощник должен это назвать
    assert "exactly ONE code in `sources`" in srv.INSTRUCTIONS
    assert "только в БДИМТ09" in srv.ingush_assistant("")
