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
        "/api/mcp-feed/v1/context": "context_home", "/api/mcp-feed/v1/check-text": "check_text",
        "/api/mcp-feed/v1/semantic-examples": "semantic_examples",
        "/api/mcp-feed/v1/translate-draft": "translate_draft",
        "/api/mcp-feed/v1/search-batch": "search_terms", "/api/mcp-feed/v1/numbers": "numbers_44",
        "/api/mcp-feed/v1/report": "report_ok", "/api/mcp-feed/v1/traps": "traps"}
ALL_TOOLS = {"search_dictionary", "search_terms", "analyze_word", "get_paradigm", "find_examples",
             "translation_context", "translate_draft", "number_to_words", "check_ingush_text", "check_spelling",
             "report_error"}


class FakeSite:
    """Внутренний HTTP сайта: записанные ответы; запоминает, что спросили."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.timeouts: list[float] = []
        self.fail: str | None = None
        self.status: dict[str, int] = {}          # путь → код ответа (429, 400…)

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
        if path in self.status:
            return httpx.Response(self.status[path], json={"ok": False})
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
    # неверная форма → ссылка на форму ошибки; report_error — только по прямой просьбе человека
    for rules in (srv.INSTRUCTIONS, text):
        assert "report_url" in rules and "сообщить об ошибке на paydadosh.ru" in rules
        assert "#feedback-form" in rules and "report_error" in rules
    assert "ONLY when the user explicitly asks" in srv.INSTRUCTIONS and "never report on your own" in srv.INSTRUCTIONS
    assert "ничего не отправляй" in text and "только если человек сам просит" in text
    # черновик перевода — всегда с пометкой
    assert "ALWAYS a draft" in srv.INSTRUCTIONS and "не проверен носителем" in text


# ── протокол целиком ───────────────────────────────────────────────────────────────────────────
def test_initialize_list_and_prompt(client, site, tmp_path):
    r = init(client)
    assert r.status_code == 200, r.text
    res = r.json()["result"]
    assert res["serverInfo"]["name"] == "paydadosh"
    assert "found=false" in res["instructions"]
    tools = rpc(client, "tools/list", {}, 2).json()["result"]["tools"]
    assert {t["name"] for t in tools} == ALL_TOOLS
    for t in tools:
        a = t["annotations"]
        assert a["title"] and a["destructiveHint"] is False and a["openWorldHint"] is False
        # пишет один report_error (сообщение в очередь редакции) — у него readOnly=false
        assert a["readOnlyHint"] is (t["name"] != "report_error"), t["name"]
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
    res = call(client, "translate_draft", {"text": "У него доброе сердце.", "direction": "ru2inh"})
    assert res["isError"] is False and res["structuredContent"]["found"] is True
    assert res["structuredContent"]["draft"] is True and "не проверен носителем" in res["structuredContent"]["draft_note"]
    assert site.timeouts[-1] == srv.MT_TIMEOUT
    assert res["structuredContent"]["word_check"]["unknown_words"] == ["хьозазаза"]
    assert _urls_everywhere(res["structuredContent"]) == []
    res = call(client, "search_terms", {"terms": ["дог", "дегаш", "хьозазаза"]})
    assert res["structuredContent"]["found_count"] == 2
    assert site.calls[-1] == ("/api/mcp-feed/v1/search-batch", {"terms": ["дог", "дегаш", "хьозазаза"],
                                                                "direction": "auto"})
    res = call(client, "number_to_words", {"number": "44"})
    assert res["structuredContent"]["cardinal"] == "шовзткъе диъ"
    assert site.calls[-1] == ("/api/mcp-feed/v1/numbers", {"n": "44"})
    res = call(client, "check_ingush_text", {"text": "Сона веза хьо кхоана. Сона веза хьо гӀулакха, хьозазаза."})
    assert res["structuredContent"]["unknown_words"] == ["хьозазаза"]
    res = call(client, "check_spelling", {"text": "Сона веза хьо гулакх"})
    assert res["structuredContent"]["found"] is True

    res = call(client, "report_error", {"word": "дог", "problem": "в примере неверный перевод",
                                        "card_url": "https://paydadosh.ru/word/104869-дог"})
    assert res["structuredContent"]["sent"] is True
    path, payload = site.calls[-1]
    assert path == "/api/mcp-feed/v1/report" and payload["client"] == "pytest-client"
    assert payload["word"] == "дог" and payload["problem"] == "в примере неверный перевод"

    rows = [r for r in journal(tmp_path) if r["tool"] != "initialize"]
    assert [r["tool"] for r in rows] == ["search_dictionary", "analyze_word", "get_paradigm", "find_examples",
                                         "translation_context", "translate_draft", "search_terms",
                                         "number_to_words", "check_ingush_text", "check_spelling", "report_error"]
    assert all(r["client"] == "pytest-client" and r["ip"] and r["found"] is True for r in rows)
    by = {r["tool"]: r for r in rows}
    assert by["translation_context"]["q"] == "Как ты себя чувствуешь?"
    # translate_draft: короткий текст (≤ 60) — в журнале, как у остальных; длина, направление, незнакомые слова
    assert by["translate_draft"]["q"] == "У него доброе сердце." and by["translate_draft"]["chars"] == 21
    assert by["translate_draft"]["direction"] == "ru2inh"
    assert by["translate_draft"]["unknown"] == ["хьозазаза"]
    assert by["search_terms"]["terms"] == 3 and by["search_terms"]["unknown"] == ["хьозазаза"]
    # из сообщения об ошибке — слово и длина, текста жалобы нет
    assert by["report_error"]["q"] == "дог" and "неверный" not in json.dumps(by["report_error"], ensure_ascii=False)
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


def test_rate_limit_message_names_the_applied_limit(client, site, monkeypatch):
    # адрес из общей сети облачного клиента — в тексте ошибки его лимит, а не лимит одиночного адреса
    srv.state().context_limiter = srv.RateLimiter(1, shared_per_minute=2)
    monkeypatch.setattr(srv, "is_shared_egress", lambda ip: True)
    for rid in (2, 3):
        assert call(client, "translation_context", {"question": f"вопрос {rid}"}, rid)["isError"] is False
    res = call(client, "translation_context", {"question": "вопрос 4"}, 4)
    assert res["isError"] is True and "Rate limit: 2 " in res["content"][0]["text"]
    monkeypatch.setattr(srv, "is_shared_egress", lambda ip: False)
    srv.state().report_limiter = srv.RateLimiter(1, shared_per_minute=30, window=3600.0, unit="hour")
    for rid in (5, 6):
        call(client, "report_error", {"word": "дог", "problem": f"ошибка {rid}"}, rid)
    res = call(client, "report_error", {"word": "дог", "problem": "ошибка 7"}, 7)
    assert "Rate limit: 1 report_error calls per hour" in res["content"][0]["text"]


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


def test_semantic_examples_shape_and_tool(client, site):
    out = srv.shape_semantic_examples(fixture("semantic_examples"), "как ты себя чувствуешь")
    assert out["translation_legend"] == srv.TRANSLATION_LEGEND
    assert out["found"] is True and out["mode"] == "semantic" and len(out["examples"]) == 2
    assert out["examples"][0]["url"] == "https://paydadosh.ru/books/read/pliev_khala_urkhe_1974/web/#p=298"
    assert out["examples"][1]["url"].endswith("&smysl=1")
    assert _urls_everywhere(out) == []

    res = call(client, "find_examples", {"word": "как ты себя чувствуешь", "mode": "semantic"})
    assert res["isError"] is False and res["structuredContent"]["mode"] == "semantic"
    assert site.calls[-1] == ("/api/mcp-feed/v1/semantic-examples", {"q": "как ты себя чувствуешь", "limit": "5"})
    # ревью 27.11: машинный перевод корпуса — с легендой и оговоркой «черновик, не проверен носителем»
    assert "не проверен носителем" in out["translation_legend"]["draft"]
    raw = fixture("semantic_examples")
    raw["items"][1]["prov"] = "machine_draft"
    draft = srv.shape_semantic_examples(raw, "как ты себя чувствуешь")
    assert draft["examples"][1]["translation"] == "draft" and "не проверен носителем" in draft["note"]
    assert "draft" not in out.get("note", "")


def test_semantic_global_budget(client, site, tmp_path):
    """Общий потолок поиска по смыслу на весь сервер: исчерпан — ответ без примеров, сайт не зовём, не в кэш."""
    srv.state().semantic_budget = srv.GlobalBudget(per_minute=1, per_day=100)
    assert call(client, "find_examples", {"word": "пойти в гости", "mode": "semantic"})["structuredContent"]["found"]
    n = len(site.calls)
    out = call(client, "find_examples", {"word": "доброе сердце", "mode": "semantic"}, rid=3)["structuredContent"]
    assert out["found"] is False and out["limited"] == "minute" and "lexical" in out["note"]
    assert len(site.calls) == n
    srv.state().semantic_budget = srv.GlobalBudget(per_minute=10, per_day=100)
    assert call(client, "find_examples", {"word": "доброе сердце", "mode": "semantic"}, rid=4)["structuredContent"]["found"]
    # сайт сказал «лимит» (его потолок: контекст + поиск по смыслу) — тот же ответ
    lim = srv.shape_semantic_examples({"available": False, "limited": True, "items": []}, "x")
    assert lim["limited"] == "minute" and "исчерпан" in lim["note"]
    # лексический поиск потолком не считается
    srv.state().semantic_budget = srv.GlobalBudget(per_minute=0, per_day=0)
    assert call(client, "find_examples", {"word": "дог"}, rid=5)["isError"] is False


def test_semantic_examples_404_graceful_and_not_cached(client, site):
    orig = MISS.pop("/api/mcp-feed/v1/semantic-examples")
    try:
        r1 = call(client, "find_examples", {"word": "пойти в гости", "mode": "semantic"})
        r2 = call(client, "find_examples", {"word": "пойти в гости", "mode": "semantic"}, rid=3)
    finally:
        MISS["/api/mcp-feed/v1/semantic-examples"] = orig
    assert r1["isError"] is False and r1["structuredContent"]["found"] is False
    assert r1["structuredContent"]["available"] is False
    assert "lexical" in r1["structuredContent"]["note"]
    assert len(site.calls) == 2   # недоступность не кэшируется


def test_translate_draft_shape_report_url_and_rate_limit(client, site, tmp_path):
    raw = fixture("translate_draft")
    out = srv.fit(srv.display(srv.shape_translate_draft(raw, truncated=False)))
    assert out["found"] is True and out["draft"] is True and "не проверен носителем" in out["draft_note"]
    assert out["translation"] == "Дог дика да цун хьозазаза."
    assert _urls_everywhere(out) == []
    cards = _card_items(out)
    assert len(cards) == 3
    for it in cards:
        assert it["report_url"] == it["url"] + "#word-report-error"
    # даже если все предложения совпали с правками редакции — всё равно черновик с пометкой
    verified = srv.shape_translate_draft({**raw, "draft": False, "all_verified_by_moderator": True}, truncated=True)
    assert verified["draft"] is True and verified["draft_note"] and "400" in verified["note"]
    assert "не ответ носителя" in verified["note"]
    empty = call(client, "translate_draft", {"text": "   "})["structuredContent"]
    assert empty["draft"] is True and empty["draft_note"]

    srv.state().mt_limiter = srv.RateLimiter(2)
    assert call(client, "translate_draft", {"text": "Первая фраза."})["isError"] is False
    assert call(client, "translate_draft", {"text": "Вторая фраза."}, rid=3)["isError"] is False
    limited = call(client, "translate_draft", {"text": "Третья фраза."}, rid=4)
    assert limited["isError"] is True and "Rate limit" in limited["content"][0]["text"]
    assert journal(tmp_path)[-1]["error"] == "rate_limited"
    # повтор из кэша лимит не тратит
    assert call(client, "translate_draft", {"text": "Первая фраза."}, rid=5)["isError"] is False


def test_translate_draft_long_text_not_logged(client, site, tmp_path):
    long = "Это длинное предложение для проверки журнала, в нём заведомо больше шестидесяти знаков."
    call(client, "translate_draft", {"text": long})
    row = journal(tmp_path)[-1]
    assert row["q"] == "" and row["chars"] == len(long) and "длинное" not in json.dumps(row, ensure_ascii=False)


def test_translate_draft_global_budget_minute_and_day(client, site, tmp_path):
    """Общий потолок на весь сервер: исчерпан — ответ без перевода (не ошибка), сайт не зовём, в кэш не кладём."""
    srv.state().mt_budget = srv.GlobalBudget(per_minute=2, per_day=100)
    for i, word in enumerate(("раз", "два")):
        assert call(client, "translate_draft", {"text": word}, rid=10 + i)["structuredContent"]["found"] is True
    n_calls = len(site.calls)
    res = call(client, "translate_draft", {"text": "три"}, rid=20)
    out = res["structuredContent"]
    assert res["isError"] is False and out["found"] is False and out["limited"] == "minute"
    assert out["translation"] == "" and out["draft"] is True and "исчерпан" in out["note"]
    assert len(site.calls) == n_calls                       # сайт (и сервис перевода) не звали
    assert journal(tmp_path)[-1]["limited"] == "minute"
    srv.state().mt_budget = srv.GlobalBudget(per_minute=100, per_day=100)
    assert call(client, "translate_draft", {"text": "три"}, rid=21)["structuredContent"]["found"] is True  # не из кэша

    day = srv.GlobalBudget(per_minute=100, per_day=3)
    assert [day.take() for _ in range(4)] == ["", "", "", "day"] and day.left_today() == 0
    srv.state().mt_budget = day
    assert call(client, "translate_draft", {"text": "четыре"}, rid=22)["structuredContent"]["limited"] == "day"


def test_global_budget_env_defaults():
    assert srv.MT_GLOBAL_PER_DAY == 500 and srv.MT_GLOBAL_PER_MINUTE == 20 and srv.MT_CONCURRENCY == 1
    assert srv.SITE_CONCURRENCY == 2 and srv.REPORTS_PER_HOUR == 5 and srv.REPORTS_PER_DAY == 50


def test_mt_lane_does_not_block_dictionary():
    """Перевод — своя очередь в одно место: пока висит перевод, словарь отвечает (общие 2 места не заняты)."""
    import asyncio

    release = asyncio.Event()
    seen: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        if request.url.path.endswith("translate-draft"):
            await release.wait()
        return httpx.Response(200, json={"ok": True})

    async def scenario():
        api = srv.SiteAPI(transport=httpx.MockTransport(handler), feed_key="", concurrency=2, mt_concurrency=1)
        t1 = asyncio.create_task(api.post("/api/mcp-feed/v1/translate-draft", {"text": "a"}, lane="mt"))
        await asyncio.sleep(0.05)
        # второй перевод ждёт своей очереди…
        t2 = asyncio.create_task(api.post("/api/mcp-feed/v1/translate-draft", {"text": "b"}, lane="mt"))
        await asyncio.sleep(0.05)
        assert seen.count("/api/mcp-feed/v1/translate-draft") == 1
        # …а два вызова словаря проходят сразу
        r = await asyncio.wait_for(asyncio.gather(api.get("/api/mcp-feed/v1/search", {"q": "дог"}),
                                                  api.get("/api/mcp-feed/v1/search", {"q": "кӀант"})), 1.0)
        assert all(x["ok"] for x in r)
        release.set()
        await asyncio.wait_for(asyncio.gather(t1, t2), 1.0)
        await api.aclose()

    asyncio.run(scenario())


def test_report_error_limits_and_cleanup(client, site, tmp_path):
    srv.state().report_limiter = srv.RateLimiter(2, window=3600.0, unit="hour")
    args = {"word": "дог", "problem": "неверный класс"}
    assert call(client, "report_error", args)["structuredContent"]["sent"] is True
    assert call(client, "report_error", args, rid=3)["structuredContent"]["sent"] is True   # не из кэша
    assert sum(1 for p, _ in site.calls if p.endswith("/report")) == 2
    res = call(client, "report_error", args, rid=4)
    assert res["isError"] is True and "per hour" in res["content"][0]["text"]
    # дневной потолок сервера — без вызова сайта
    srv.state().report_limiter = srv.RateLimiter(100, window=3600.0, unit="hour")
    srv.state().report_budget = srv.GlobalBudget(100, 0)
    out = call(client, "report_error", args, rid=5)["structuredContent"]
    assert out["sent"] is False and out["limited"] == "day" and "#feedback-form" in out["url"]
    assert sum(1 for p, _ in site.calls if p.endswith("/report")) == 2
    # сайт ответил 429 (его сутки) — тоже понятный ответ
    srv.state().report_budget = srv.GlobalBudget(100, 100)
    site.status["/api/mcp-feed/v1/report"] = 429
    assert call(client, "report_error", args, rid=6)["structuredContent"]["limited"] == "day"
    # без слова, карточки и предложения — не отправляем
    site.status.clear()
    out = call(client, "report_error", {"problem": "что-то не так"}, rid=7)["structuredContent"]
    assert out["sent"] is False and "word" in out["note"]
    # потолок сервера тратит только ушедшее сообщение: отказ сайта (400/429/сбой) и дубль его возвращают
    budget = srv.state().report_budget = srv.GlobalBudget(100, 1)
    site.status["/api/mcp-feed/v1/report"] = 400
    assert call(client, "report_error", args, rid=8)["structuredContent"]["sent"] is False
    assert budget.left_today() == 1
    site.status.clear()
    assert call(client, "report_error", args, rid=9)["structuredContent"]["sent"] is True
    assert budget.left_today() == 0
    budget.refund()
    budget.refund()                                      # лишний возврат не уводит счёт в минус
    assert budget.left_today() == 1


def test_search_terms_shape_and_units(client, site):
    out = srv.shape_search_terms(fixture("search_terms"), ["дог", "дегаш", "хьозазаза"])
    assert out["count"] == 3 and out["found_count"] == 2 and _urls_everywhere(out) == []
    by = {r["term"]: r for r in out["items"]}
    assert by["дог"]["headword"] == "дог" and by["дог"]["translation"] == "сердце"
    assert by["дог"]["report_url"].endswith("#word-report-error")
    assert by["дегаш"]["headword"] == "дог" and "словоформа" in by["дегаш"]["note"]
    assert by["хьозазаза"]["found"] is False and by["хьозазаза"]["suggestions_note"] == "возможно, имелось в виду"
    # лимит — в единицах ⌈N/5⌉: 10 единиц — это 50 слов одним вызовом, следующий не проходит
    srv.state().batch_limiter = srv.RateLimiter(10)
    assert call(client, "search_terms", {"terms": [f"слово{i}" for i in range(50)]})["isError"] is False
    res = call(client, "search_terms", {"terms": ["дог"]}, rid=3)
    assert res["isError"] is True and "units" in res["content"][0]["text"]
    lim = srv.RateLimiter(5)
    assert lim.allow("a", cost=3) and not lim.allow("a", cost=3) and lim.allow("a", cost=2)
    too_many = call(client, "search_terms", {"terms": ["x"] * 51}, rid=4)
    assert too_many["isError"] is True
    # не успели за время пакета — skipped (это не «нет в словаре»), такой ответ не кэшируется
    raw = {"items": [*fixture("search_terms")["items"], {"q": "кӀант", "found": False, "skipped": True}]}
    out = srv.shape_search_terms(raw, ["дог", "дегаш", "хьозазаза", "кӀант"])
    by = {r["term"]: r for r in out["items"]}
    assert by["кӀант"]["skipped"] is True and out["skipped_count"] == 1 and "skipped=true" in out["note"]
    assert "found=false" in out["note"]                           # «хьозазаза» — настоящий промах
    assert srv.BATCH_UNITS_PER_MINUTE == 20 and srv.SHARED_BATCH_UNITS_PER_MINUTE == 60


def test_batch_lane_does_not_block_dictionary():
    """Пакет — своя очередь в одно место: пока висит пакет, словарь отвечает."""
    import asyncio

    release = asyncio.Event()
    seen: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        if request.url.path.endswith("search-batch"):
            await release.wait()
        return httpx.Response(200, json={"ok": True})

    async def scenario():
        api = srv.SiteAPI(transport=httpx.MockTransport(handler), feed_key="", concurrency=2, batch_concurrency=1)
        t1 = asyncio.create_task(api.post("/api/mcp-feed/v1/search-batch", {"terms": ["a"]}, lane="batch"))
        t2 = asyncio.create_task(api.post("/api/mcp-feed/v1/search-batch", {"terms": ["b"]}, lane="batch"))
        await asyncio.sleep(0.05)
        assert seen.count("/api/mcp-feed/v1/search-batch") == 1
        r = await asyncio.wait_for(asyncio.gather(api.get("/api/mcp-feed/v1/search", {"q": "дог"}),
                                                  api.get("/api/mcp-feed/v1/search", {"q": "кӀант"})), 1.0)
        assert all(x["ok"] for x in r)
        release.set()
        await asyncio.wait_for(asyncio.gather(t1, t2), 1.0)
        await api.aclose()

    asyncio.run(scenario())


def test_numbers_shape():
    out = srv.shape_numbers(fixture("numbers_44"))
    assert out["cardinal"] == "шовзткъе диъ" and out["ordinal"] == "шовзткъе диълагӏа"
    assert out["url"] == "https://paydadosh.ru/numbers" and "класса д" in out["class_note"]
    # ревью 27.11: 4, 14, 80 и составные меняются по классу — не «количественные по классам не меняются»
    assert out["class_variable"] == ["диъ", "диълагӏа"] and "не меняются" not in out["class_note"]
    assert "class_warning" not in out
    warn = srv.shape_numbers({**fixture("numbers_44"), "class": "в"})
    assert "класса в" in warn["class_warning"] and "диъ" in warn["class_warning"]
    miss = srv.shape_numbers({"n": "1" + "0" * 21, "found": False, "error": "too_large"})
    assert miss["found"] is False and "10^21" in miss["note"]


def test_number_to_words_schema_string_and_any_class(client, site):
    init(client)
    tools = {t["name"]: t for t in rpc(client, "tools/list", {}, 3).json()["result"]["tools"]}
    props = tools["number_to_words"]["inputSchema"]["properties"]
    assert props["number"]["type"] == "string" and props["number"]["pattern"] == r"^\d{1,21}$"
    assert "" not in props["noun_class"]["enum"] and props["noun_class"]["default"] == "any"
    # целое из JSON тоже принимается (как строка); 21 цифра — без округления
    call(client, "number_to_words", {"number": 44}, rid=4)
    assert site.calls[-1] == ("/api/mcp-feed/v1/numbers", {"n": "44"})
    call(client, "number_to_words", {"number": "999999999999999999999", "noun_class": "в"}, rid=5)
    assert site.calls[-1] == ("/api/mcp-feed/v1/numbers", {"n": "999999999999999999999", "cls": "в"})
    assert call(client, "number_to_words", {"number": "1e21"}, rid=6)["isError"] is True
    assert call(client, "number_to_words", {"number": "-5"}, rid=7)["isError"] is True


def test_did_you_mean_on_misses():
    out = srv.shape_search(fixture("search_miss"), "хьозазаза")
    if out.get("suggestions"):
        assert out["suggestions_note"] == "возможно, имелось в виду" and "возможно, имелось в виду" in out["note"]
    raw = {"entries": [], "suggestions": ["дог", "дагь"]}
    out = srv.shape_search(raw, "дуог")
    assert [s["word"] for s in out["suggestions"]] == ["дог", "дагь"] and out["suggestions_note"]
    assert "Возможно, имелось в виду: дог, дагь" in out["note"]
    ana = srv.shape_analysis(fixture("razbor_miss"), "хьозазаза")
    assert ana["suggestions_note"] == "возможно, имелось в виду" and "имелось в виду" in ana["note"]
    # ревью 27.11: строка карточки «заголовок (формы) — толкование» → голый заголовок, и в ссылке тоже
    out = srv.shape_search({"entries": [], "suggestions": ["дог (дийна, дега) — сердце", "дог — сердце", "дагь"]}, "дуог")
    assert [s["word"] for s in out["suggestions"]] == ["дог", "дагь"]
    assert out["suggestions"][0]["url"] == "https://paydadosh.ru/?q=%D0%B4%D0%BE%D0%B3"


def test_context_parity_blocks():
    raw = fixture("context_feel")
    raw.update({
        "ready_answers": [{"kind": "phrasebook", "source": "разговорник", "use": "verbatim",
                           "text": "Мишта хет хьона? — Как ты себя чувствуешь? [разговорник]", "path": "/phrasebook/1-x"},
                          {"kind": "qa", "source": "«Хаттараш»", "use": "reference", "text": "вопрос …", "path": "/ask/5"}],
        "paradigms": "Парадигмы слов (формы из словаря…):\n  • дог (сущ.) — …",
        "article_examples": ["пример из статьи"],
        "stories": [{"ing": "абзац", "ru": "абзац", "title": "Рассказ", "path": "/stories"}],
        "untranslated_corpus": [{"ing": "предложение", "source": "КОД21", "note": "…"}],
        "dictionary": [{"term": "дог", "definitions": ["сердце"], "sources": [], "path": "/word/1-дог",
                        "examples": [{"ing": "Дог дика да цун", "ru": "У него доброе сердце"}]}],
    })
    out = srv.shape_context(raw, "Как ты себя чувствуешь?")
    assert out["ready_answers"][0]["url"] == "https://paydadosh.ru/phrasebook/1-x"
    assert out["ready_answers"][1]["url"] == "https://paydadosh.ru/ask/5"
    assert out["paradigms"].startswith("Парадигмы слов") and out["article_examples"] and out["stories"]
    assert out["untranslated_corpus"][0]["url"].startswith("https://paydadosh.ru/corpus?q=")
    assert out["dictionary"][0]["examples"][0]["url"] == out["dictionary"][0]["url"]
    assert out["order"].startswith("ready_answers") and _urls_everywhere(out) == []


def test_traps_flag_off_by_default(client, site, monkeypatch):
    assert srv.EXPOSE_TRAPS is False
    got = rpc(client, "prompts/get", {"name": "ingush_assistant", "arguments": {}}, 4).json()
    text = got["result"]["messages"][0]["content"]["text"]
    assert srv.TRAPS_HEADING.upper() not in text
    assert not any(p.endswith("/traps") for p, _ in site.calls)          # сайт не спрашивали
    monkeypatch.setattr(srv, "EXPOSE_TRAPS", True)
    got = rpc(client, "prompts/get", {"name": "ingush_assistant", "arguments": {}}, 5).json()
    text = got["result"]["messages"][0]["content"]["text"]
    assert srv.TRAPS_HEADING.upper() in text and "время (й)" in text
    rpc(client, "prompts/get", {"name": "ingush_assistant", "arguments": {}}, 6)
    assert sum(1 for p, _ in site.calls if p.endswith("/traps")) == 1     # кэш на час


# Внутренние имена и адреса в публичных файлах: сам список здесь НЕ лежит ни строкой, ни хэшем (короткие строки по
# хэшу восстанавливаются перебором за секунды). Его задаёт окружение MCP_FORBIDDEN_STRINGS (через запятую) — CI и
# проверка перед публикацией закрытого репозитория; без переменной сторож пропускается.
def _forbidden_strings() -> list[str]:
    import os

    return [x for x in (y.strip().lower() for y in os.getenv("MCP_FORBIDDEN_STRINGS", "").split(",")) if x]


def _forbidden_hits(text: str, forbidden: list[str]) -> list[str]:
    """Позиции запретных строк в тексте (без регистра); сами строки в ответ не попадают."""
    low = text.lower()
    return [f"#{n}@{low.index(x)}" for n, x in enumerate(forbidden) if x in low]


def _public_files(root: Path) -> list[Path]:
    skip = {".git", ".venv", "venv", "__pycache__", ".pytest_cache", "data"}
    return sorted(p for p in root.rglob("*") if p.is_file() and not skip & set(p.relative_to(root).parts))


def test_no_internal_infra_mentions_in_public_mcp_server():
    forbidden = _forbidden_strings()
    if not forbidden:
        pytest.skip("MCP_FORBIDDEN_STRINGS не задана")
    for path in _public_files(HERE):
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        assert _forbidden_hits(text, forbidden) == [], path.relative_to(HERE)


def test_forbidden_guard_works(tmp_path):
    # сама проверка: строка ловится в любом регистре; служебные каталоги не читаются
    assert _forbidden_hits("text QwXyZ text", ["qwxyz"]) == ["#0@5"]
    assert _forbidden_hits("paydadosh.ru", ["qwxyz"]) == []
    (tmp_path / ".venv").mkdir()
    (tmp_path / ".venv" / "x.py").write_text("qwxyz", encoding="utf-8")
    (tmp_path / ".gitignore").write_text("data/", encoding="utf-8")
    assert [p.name for p in _public_files(tmp_path)] == [".gitignore"]
