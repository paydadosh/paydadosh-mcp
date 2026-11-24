"""Живая проверка MCP-сервера PaydaDosh официальным клиентом SDK (локально или прод).

    .venv/bin/python smoke.py http://127.0.0.1:8010/mcp          # все инструменты + 100 вызовов подряд
    .venv/bin/python smoke.py https://paydadosh.ru/mcp --series 20
    .venv/bin/python smoke.py https://paydadosh.ru/mcp --burst 90  # лимит nginx: после ~21 запроса — 429

Печатает по строке на вызов (isError, found, мс, байты) и сводку серии. Выход 1, если была ошибка.
⚠ Лимит проверять НЕ с самого сервера: там все запросы приходят с одного адреса вместе с прочими
проверками. И с одного адреса: облачные клиенты (Claude, ChatGPT) ходят со своих общих адресов.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

CHECKS = [
    ("search_dictionary", {"query": "хлеб"}, "маькх"),
    ("search_dictionary", {"query": "отец"}, "да"),
    ("search_dictionary", {"query": "вода"}, "хи"),
    ("search_dictionary", {"query": "кIант"}, "кӀант"),             # палочка латинской I
    ("search_dictionary", {"query": "хьозазаза"}, None),             # нет в словаре → found=false
    ("analyze_word", {"word": "дегаш"}, "дог"),
    ("get_paradigm", {"word": "да", "part_of_speech": "noun"}, "дас"),
    ("get_paradigm", {"word": "ала", "part_of_speech": "verb"}, "аьлар"),
    ("find_examples", {"word": "дог"}, "дог"),
    ("translation_context", {"question": "Как ты себя чувствуешь?"}, "хьона"),
    ("check_ingush_text", {"text": "Сона хьо веза, хьо са дог ва. хьозазаза"}, "хьозазаза"),
    ("check_spelling", {"text": "Со гIалгIай ва, гулакх"}, "гӀулакх"),
]
REPORT_TOOLS = {"search_dictionary", "analyze_word", "get_paradigm"}   # у найденного — форма ошибки карточки
SERIES_WORDS = ["дог", "кӀант", "ала", "дегаш", "сердце", "вода", "хий", "нана", "да", "бер",
                "цӀа", "малх", "латта", "хьо", "со", "шу", "вай", "бӀарг", "кулг", "маькх"]


async def run(url: str, series: int) -> int:
    bad = 0
    async with streamablehttp_client(url) as (read, write, _):
        async with ClientSession(read, write) as session:
            init = await session.initialize()
            print(f"server {init.serverInfo.name} {init.serverInfo.version}, protocol {init.protocolVersion}")
            print("tools:", ", ".join(t.name for t in (await session.list_tools()).tools))
            for tool, args, expect in CHECKS:
                t = time.perf_counter()
                res = await session.call_tool(tool, args)
                ms = round((time.perf_counter() - t) * 1000)
                text = res.content[0].text if res.content else ""
                data = res.structuredContent or {}
                ok = not res.isError and (expect is None and data.get("found") is False
                                          or expect is not None and data.get("found") and expect in text)
                # строка источника со ссылкой на paydadosh.ru — в каждом ответе
                kinds = [c.type for c in res.content or []]
                ok = ok and len(kinds) > 1 and res.content[1].text.startswith("Источник: PaydaDosh — https://")
                if ok and tool in REPORT_TOOLS and data.get("found"):
                    ok = "#word-report-error" in text
                bad += not ok
                print(f"{'ok ' if ok else 'BAD'} {tool} {json.dumps(args, ensure_ascii=False)} found={data.get('found')} "
                      f"{ms} ms {len(text.encode())} B {'+'.join(kinds)}{'' if ok else '  ' + text[:200]}")
            lat, errors = [], 0
            for i in range(series):
                tool = ("search_dictionary", "analyze_word", "get_paradigm", "find_examples")[i % 4]
                word = SERIES_WORDS[i % len(SERIES_WORDS)]
                t = time.perf_counter()
                res = await session.call_tool(tool, {"query": word} if tool == "search_dictionary" else {"word": word})
                lat.append((time.perf_counter() - t) * 1000)
                if res.isError:
                    errors += 1
                    print("ERROR", tool, word, res.content[0].text[:200])
            if series:
                lat.sort()
                print(f"series {series}: errors={errors}, p50={statistics.median(lat):.0f} ms, "
                      f"p95={lat[max(0, int(len(lat) * 0.95) - 1)]:.0f} ms, max={lat[-1]:.0f} ms")
            bad += errors
    return 1 if bad else 0


def burst(url: str, n: int) -> int:
    body = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    headers = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}
    codes: dict[int, int] = {}
    with httpx.Client(timeout=20) as c:
        for _ in range(n):
            code = c.post(url, json=body, headers=headers).status_code
            codes[code] = codes.get(code, 0) + 1
    print("burst", n, "→", dict(sorted(codes.items())))
    return 0 if 429 in codes else 1


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("url", nargs="?", default="http://127.0.0.1:8010/mcp")
    ap.add_argument("--series", type=int, default=100)
    ap.add_argument("--burst", type=int, default=0, help="N быстрых tools/list подряд: ждём 429")
    args = ap.parse_args()
    if args.burst:
        return burst(args.url, args.burst)
    return asyncio.run(run(args.url, args.series))


if __name__ == "__main__":
    raise SystemExit(main())
