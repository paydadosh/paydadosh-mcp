# -*- coding: utf-8 -*-
"""Отчёт по журналу MCP-сервера PaydaDosh.

    python mcp_stats.py <каталог журнала | файл.jsonl ...> [--days 30] [--top 30] [--json]

Журнал — дневные файлы `mcp_calls-ГГГГ-ММ-ДД.jsonl` (строка на вызов: ts, tool, q, found, ms, client,
ip — хэш с солью, cached, error; у проверок текста — `chars` и `unknown`, незнакомые словарю слова).

Что считает:
* подключения (`initialize`) и вызовы по дням, разных адресов (по хэшу);
* по инструментам: вызовы, «не найдено» и его доля, ошибки сайта, попадания в кэш, задержка p50 / p95;
* клиенты (по вызовам и по подключениям);
* частые вопросы (`translation_context`, `search_dictionary`) — что люди спрашивают;
* слова, которых нет в словаре, со счётом — список пробелов: незнакомые слова из проверок текста плюс
  запросы поиска и разбора, на которые ничего не нашлось;
* «не нашлось» по каждому инструменту отдельно.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError):
        pass

QUESTION_TOOLS = ("translation_context", "search_dictionary")
LOOKUP_TOOLS = ("search_dictionary", "analyze_word", "get_paradigm", "find_examples")


def journal_files(paths: Iterable[Path]) -> list[Path]:
    out: list[Path] = []
    for p in paths:
        p = Path(p)
        if p.is_dir():
            out += sorted(p.glob("mcp_calls*.jsonl"))
        elif p.exists():
            out.append(p)
    return out


def read_rows(paths: Iterable[Path] | Path, days: int | None = None) -> list[dict[str, Any]]:
    if isinstance(paths, (str, Path)):
        paths = [Path(paths)]
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ") if days else ""
    rows: list[dict[str, Any]] = []
    for path in journal_files(paths):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except ValueError:
                    continue  # недописанная строка при обрыве — пропускаем
                if not isinstance(row, dict) or (since and str(row.get("ts") or "") < since):
                    continue
                rows.append(row)
    rows.sort(key=lambda r: str(r.get("ts") or ""))
    return rows


def _pct(values: list[float], q: float) -> int:
    if not values:
        return 0
    values = sorted(values)
    return round(values[min(len(values) - 1, int(q * len(values)))])


def _key(q: Any) -> str:
    return " ".join(str(q or "").split()).strip().lower()


def summarize(rows: Iterable[dict[str, Any]], top: int = 30) -> dict[str, Any]:
    rows = list(rows)
    calls = [r for r in rows if r.get("tool") != "initialize"]
    inits = [r for r in rows if r.get("tool") == "initialize"]
    by_day: dict[str, Counter] = defaultdict(Counter)
    ips_by_day: dict[str, set] = defaultdict(set)
    for r in rows:
        day = str(r.get("ts") or "")[:10]
        by_day[day]["initialize" if r.get("tool") == "initialize" else "calls"] += 1
        if r.get("ip"):
            ips_by_day[day].add(r["ip"])
    tools: dict[str, dict[str, Any]] = {}
    not_found: dict[str, list[tuple[str, int]]] = {}
    for name in sorted({str(r.get("tool")) for r in calls}):
        mine = [r for r in calls if r.get("tool") == name]
        answered = [r for r in mine if not r.get("error")]
        miss = [r for r in answered if r.get("found") is False]
        fresh = [float(r.get("ms") or 0) for r in answered if not r.get("cached")]
        tools[name] = {
            "calls": len(mine),
            "not_found": len(miss),
            "not_found_share": round(len(miss) / len(answered), 3) if answered else 0.0,
            "errors": len(mine) - len(answered),
            "cached": sum(1 for r in mine if r.get("cached")),
            "p50_ms": _pct(fresh, 0.5),
            "p95_ms": _pct(fresh, 0.95),
        }
        nf = Counter(_key(r.get("q")) for r in miss if _key(r.get("q")))
        if nf:
            not_found[name] = nf.most_common(top)
    questions = Counter(_key(r.get("q")) for r in calls
                        if r.get("tool") in QUESTION_TOOLS and not r.get("error") and _key(r.get("q")))
    # пробелы словаря: незнакомые слова проверок текста + запросы без результата
    gaps: Counter = Counter()
    for r in calls:
        for w in r.get("unknown") or []:
            if _key(w):
                gaps[_key(w)] += 1
        if r.get("tool") in LOOKUP_TOOLS and r.get("found") is False and not r.get("error") and _key(r.get("q")):
            gaps[_key(r.get("q"))] += 1
    errors = Counter(str(r.get("error")) for r in calls if r.get("error"))
    return {
        "period": [min((str(r.get("ts")) for r in rows), default=""), max((str(r.get("ts")) for r in rows), default="")],
        "calls": len(calls),
        "connections": len(inits),
        "unique_ips": len({r.get("ip") for r in rows if r.get("ip")}),
        "by_day": {d: {"calls": c["calls"], "connections": c["initialize"], "ips": len(ips_by_day[d])}
                   for d, c in sorted(by_day.items())},
        "tools": tools,
        "clients": dict(Counter(str(r.get("client") or "unknown") for r in calls).most_common()),
        "connections_by_client": dict(Counter(str(r.get("client") or "unknown") for r in inits).most_common()),
        "errors": dict(errors.most_common()),
        "top_questions": questions.most_common(top),
        "dictionary_gaps": gaps.most_common(top),
        "not_found_by_tool": not_found,
    }


def render(s: dict[str, Any]) -> str:
    out = [f"Период: {s['period'][0]} — {s['period'][1]}",
           f"Вызовов: {s['calls']}, подключений: {s['connections']}, разных адресов: {s['unique_ips']}", "",
           "По дням (вызовы / подключения / адреса):"]
    out += [f"  {d}  {v['calls']:>6}  {v['connections']:>5}  {v['ips']:>5}" for d, v in s["by_day"].items()]
    out += ["", "Инструменты:          вызовы  не найдено   доля  ошибки   кэш   p50   p95"]
    for name, t in s["tools"].items():
        out.append(f"  {name:<20} {t['calls']:>6} {t['not_found']:>10} {t['not_found_share']:>6.1%} "
                   f"{t['errors']:>6} {t['cached']:>5} {t['p50_ms']:>5} {t['p95_ms']:>5}")
    out += ["", "Клиенты (вызовы): " + ", ".join(f"{k} {v}" for k, v in s["clients"].items())]
    if s["connections_by_client"]:
        out.append("Клиенты (подключения): " + ", ".join(f"{k} {v}" for k, v in s["connections_by_client"].items()))
    if s["errors"]:
        out.append("Ошибки: " + ", ".join(f"{k} {v}" for k, v in s["errors"].items()))
    if s["top_questions"]:
        out += ["", "Частые вопросы и запросы:"]
        out += [f"  {n:>4}  {q}" for q, n in s["top_questions"]]
    if s["dictionary_gaps"]:
        out += ["", "Нет в словаре (кандидаты в словарь):"]
        out += [f"  {n:>4}  {q}" for q, n in s["dictionary_gaps"]]
    for tool, items in s["not_found_by_tool"].items():
        out += ["", f"Не нашлось — {tool}:"]
        out += [f"  {n:>4}  {q}" for q, n in items]
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("paths", type=Path, nargs="+", help="каталог журнала или файлы .jsonl")
    ap.add_argument("--days", type=int, default=None, help="только последние N дней")
    ap.add_argument("--top", type=int, default=30, help="сколько строк в списках")
    ap.add_argument("--json", action="store_true", help="машинный вывод")
    args = ap.parse_args(argv)
    files = journal_files(args.paths)
    if not files:
        print("журнала нет: " + ", ".join(str(p) for p in args.paths), file=sys.stderr)
        return 1
    s = summarize(read_rows(files, args.days), args.top)
    print(json.dumps(s, ensure_ascii=False, indent=1) if args.json else render(s))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
