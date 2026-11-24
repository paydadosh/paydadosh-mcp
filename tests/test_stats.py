"""Отчёт по журналу (`mcp_stats.py`): дни, инструменты, клиенты, частые вопросы, пробелы словаря."""
import importlib.util
import json
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("paydadosh_mcp_stats", HERE / "mcp_stats.py")
stats = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stats)


def rows():
    return [
        {"ts": "2026-10-22T10:00:00Z", "tool": "initialize", "client": "claude-ai", "ip": "a1"},
        {"ts": "2026-10-22T10:00:01Z", "tool": "search_dictionary", "q": "дог", "found": True, "ms": 200,
         "client": "claude-ai", "ip": "a1", "cached": False},
        {"ts": "2026-10-22T10:00:02Z", "tool": "search_dictionary", "q": "Хьозаза", "found": False, "ms": 150,
         "client": "claude-ai", "ip": "a1", "cached": False},
        {"ts": "2026-10-22T10:00:03Z", "tool": "translation_context", "q": "Как сказать я хочу домой",
         "found": True, "ms": 4000, "client": "claude-ai", "ip": "a1", "cached": False},
    ], [
        {"ts": "2026-10-23T09:00:00Z", "tool": "search_dictionary", "q": "хьозаза", "found": False, "ms": 5,
         "client": "cursor", "ip": "b2", "cached": True},
        {"ts": "2026-10-23T09:00:01Z", "tool": "analyze_word", "q": "дегаш", "found": None, "ms": 8000,
         "client": "cursor", "ip": "b2", "cached": False, "error": "timeout"},
        {"ts": "2026-10-23T09:00:02Z", "tool": "check_ingush_text", "q": "", "found": True, "ms": 300,
         "client": "cursor", "ip": "b2", "cached": False, "chars": 40, "unknown": ["Хьозаза", "кхелла"]},
        {"ts": "2026-10-23T09:00:03Z", "tool": "translation_context", "q": "как сказать  я хочу домой",
         "found": True, "ms": 3000, "client": "cursor", "ip": "b2", "cached": False},
    ]


def write_days(tmp_path):
    d1, d2 = rows()
    (tmp_path / "mcp_calls-2026-10-22.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in d1) + "\n", encoding="utf-8")
    (tmp_path / "mcp_calls-2026-10-23.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in d2) + "\n{обрыв", encoding="utf-8")


def test_summary_over_daily_files(tmp_path):
    write_days(tmp_path)
    s = stats.summarize(stats.read_rows(tmp_path))
    assert s["calls"] == 7 and s["connections"] == 1 and s["unique_ips"] == 2
    assert s["by_day"]["2026-10-22"] == {"calls": 3, "connections": 1, "ips": 1}
    sd = s["tools"]["search_dictionary"]
    assert sd["calls"] == 3 and sd["not_found"] == 2 and sd["cached"] == 1
    assert sd["p50_ms"] in (150, 200)                   # попадание в кэш в задержку не идёт
    assert s["tools"]["analyze_word"]["errors"] == 1 and s["errors"] == {"timeout": 1}
    # частые вопросы: пробелы и регистр запрос не делят
    assert ("как сказать я хочу домой", 2) in s["top_questions"]
    # пробелы словаря: «хьозаза» — дважды из поиска и раз из проверки текста
    gaps = dict(s["dictionary_gaps"])
    assert gaps["хьозаза"] == 3 and gaps["кхелла"] == 1
    assert s["not_found_by_tool"]["search_dictionary"] == [("хьозаза", 2)]
    assert s["clients"] == {"cursor": 4, "claude-ai": 3}
    text = stats.render(s)
    assert "Нет в словаре" in text and "кхелла" in text and "Частые вопросы" in text


def test_main(tmp_path, capsys):
    write_days(tmp_path)
    assert stats.main([str(tmp_path), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["calls"] == 7
    assert stats.main([str(tmp_path / "нет")]) == 1
