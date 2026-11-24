# PaydaDosh MCP server — Ingush dictionary for AI assistants

An open [Model Context Protocol](https://modelcontextprotocol.io) server for the Ingush language, backed by
[PaydaDosh](https://paydadosh.ru), an Ingush–Russian dictionary: tens of thousands of headwords merged from dozens
of printed dictionaries, Ozdoev's Russian–Ingush dictionary, a corpus of Ingush texts with Russian translations, a
morphological analyzer and a spell-checker.

Connect it to Claude, ChatGPT, Cursor or any MCP client and the assistant takes Ingush words and forms from the
dictionary instead of inventing them. The server only returns data — the reasoning is done by your model.

**Public endpoint:** `https://mcp.paydadosh.ru/mcp` · Streamable HTTP · no authentication · read-only.
More on the site: <https://paydadosh.ru/developers>.

## Connect

| Client | How |
|---|---|
| Claude (web / desktop) | Settings → Connectors → *Add custom connector* → name `PaydaDosh`, URL `https://mcp.paydadosh.ru/mcp` |
| Claude Code | `claude mcp add --transport http paydadosh https://mcp.paydadosh.ru/mcp` |
| ChatGPT | Settings → Apps & Connectors → Developer mode → *Create* → URL `https://mcp.paydadosh.ru/mcp`, no auth |
| Cursor | `~/.cursor/mcp.json`: `{"mcpServers": {"paydadosh": {"url": "https://mcp.paydadosh.ru/mcp"}}}` |
| VS Code | `.vscode/mcp.json`: `{"servers": {"paydadosh": {"type": "http", "url": "https://mcp.paydadosh.ru/mcp"}}}` |
| stdio-only clients | `npx -y mcp-remote https://mcp.paydadosh.ru/mcp` |

Inspect it by hand: `npx @modelcontextprotocol/inspector` → transport *Streamable HTTP* → the URL above.

## Tools

| Tool | What it does | Example |
|---|---|---|
| `search_dictionary` | Ingush ↔ Russian lookup; inflected Russian is reduced to the dictionary form; headwords with part of speech, definition, noun class, source codes, link; the first headword also has its attested principal forms and 1–2 card examples; Ozdoev's dictionary for Russian queries; an inflected Ingush form points to its lemma | `дог`, `кӀант`, `сердце`, `закрой` |
| `analyze_word` | Lemma, part of speech, grammatical description, confidence, morpheme segmentation, other readings, up to 3 corpus examples | `дегаш` → plural of `дог` "heart" |
| `get_paradigm` | The full inflection table; every form has a reliability tier: `verified`, `dictionary`, `corpus`, `rule` (generated, may be wrong), `auto`; verb rows carry the class row `cls` {в, й, б, д}, nouns — `noun_class` (singular / plural); `main_only` for the principal rows | `дог`, `ала` (verb) |
| `find_examples` | Up to 5 corpus sentences with translation and source (first page only) | `бӀаргаш` |
| `translation_context` | What the site's own assistant reads before answering a question: native-speaker corrections, phrasebook, parallel sentences, dictionary entries, grammar notes, answers from the Q&A section «Хаттараш». Call it first for phrase translation | `Как сказать «я хочу домой»?` |
| `check_ingush_text` | Verify an Ingush text word by word (typically the assistant's own draft) with the same check the site's assistant applies to its answers: `confirmed` / `unverified` (likely invented, with suggestions); unverifiable quotes; card link only when unambiguous | `Сона хьо веза, хьо са дог ва` |
| `check_spelling` | Spell-check a person's text: errors with suggestions, wrong palochka letter, class agreement | `Со гӀалгӀай ва` |

Prompt: `ingush_assistant` ("Ingush assistant / Ингушский помощник") — which tool first, never invent forms,
cite sources, link every Ingush word to its paydadosh.ru page, and the most common grammar traps.

Every result has `found` (and a short `note` when nothing was found), a `url` to the page on paydadosh.ru for
every item, and source codes of the printed editions (БДИМТ09, КУР05, ОЗД80…). The text content ends with a
line `Источник: PaydaDosh — <url>`; clients on protocol 2025-06-18+ also get a `resource_link`. The palochka is
returned as `Ӏ` (U+04C0); input accepts `I`, `1`, `l` and `|`. Definitions and notes are in Russian, as on the site.

Every item that has a word page (search results, analyses, the paradigm, words of `check_ingush_text` with an
unambiguous card) also has `report_url` — the «Сообщить об ошибке в карточке» form on that page
(`…/word/<id>-<slug>#word-report-error`). When a user says a form or translation is wrong, the assistant gives
them this link; the server itself accepts no reports.

## Limits

- 60 requests per minute per IP (HTTP 429 above that); `translation_context` — 20 per minute.
- A tool result is at most ~20 KB; long lists are cut and marked `truncated`.
- Corpus: at most 5 sentences of ≤ 300 characters per call, first page of results only.
- Read-only: no writes, no account, no bulk export of the dictionary or the corpus.

## Data and licence

The code in this repository is MIT-licensed (see `LICENSE`). **The data is not**: the dictionary, corpus,
phrasebook and answers belong to paydadosh.ru and to the authors and publishers of the source editions. Corpus
sentences are short quotations from copyrighted publications — cite the source when you use them. The results
are dictionary data, not a check by a native speaker.

## Privacy

One line per tool call, kept for 90 days (one file per day, older files are deleted): time, tool, the query
(up to 300 characters for `search_dictionary` and `translation_context`, up to 64 for the others), whether
anything was found, duration, the client name (from the MCP handshake or the User-Agent) and a salted hash of
the IP address — the address itself is not stored. For `check_ingush_text` and `check_spelling` the text is not
stored: only the words the dictionary does not know (up to 30) and the length. No names, no contacts, no
cookies. The log is used for statistics and to find words missing from the dictionary. Questions and requests
to delete log records — the feedback form <https://paydadosh.ru/#feedback-form> or admin@paydadosh.ru; other
channels are under [Contact](#contact).

## Contact

- **A wrong word, form or translation** — the error form on the word page («Сообщить об ошибке в карточке»;
  the `report_url` of any result links straight to it).
- **Questions, suggestions, a message to the editors** — the feedback form on the home page:
  <https://paydadosh.ru/#feedback-form> (type «Сообщение редакции»), or the project's email
  [admin@paydadosh.ru](mailto:admin@paydadosh.ru).
- **Telegram** — the bot [@PaydaDosh_Bot](https://t.me/PaydaDosh_Bot) (send a word, an example or a question);
  project news — the channel [@paydadosh](https://t.me/paydadosh).
- **Developers** — bugs and feature requests for the server itself: GitHub Issues,
  https://github.com/paydadosh/paydadosh-mcp/issues.

## Self-hosting / development

The server is a small FastMCP application (Python 3.10+, `mcp==1.30.0`). It does not contain the dictionary: it
calls the internal HTTP API of the PaydaDosh site (`MCP_SITE_API`).

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
MCP_SITE_API=http://127.0.0.1:8000 MCP_IP_SALT=change-me .venv/bin/python server.py   # 127.0.0.1:8010/mcp
.venv/bin/pip install pytest==9.1.1 && .venv/bin/python -m pytest                      # tests use recorded responses
.venv/bin/python smoke.py http://127.0.0.1:8010/mcp --series 100                       # live check
python mcp_stats.py data/ --days 30                                                    # call log report
```

| Variable | Default | Meaning |
|---|---|---|
| `MCP_SITE_API` | `http://127.0.0.1:8000` | internal address of the PaydaDosh site |
| `MCP_PUBLIC_SITE` | `https://paydadosh.ru` | base of the links in results |
| `MCP_HOST` / `MCP_PORT` | `127.0.0.1` / `8010` | listen address |
| `MCP_LOG_DIR` | `./data` | call log directory |
| `MCP_LOG_RETENTION_DAYS` | `90` | days to keep the log |
| `MCP_IP_SALT` | random per process | salt of the IP hash — keep it secret, outside the repository |
| `MCP_FEED_KEY` | — | shared key sent to the site as `X-MCP-Feed-Key` (if the site requires it) |
| `MCP_API_TIMEOUT` / `MCP_CONTEXT_TIMEOUT` | `8` / `15` | seconds per site call |
| `MCP_SITE_CONCURRENCY` | `2` | concurrent site calls |
| `MCP_CONTEXT_PER_MINUTE` | `20` | `translation_context` calls per minute per IP |
| `MCP_ALLOWED_HOSTS` / `MCP_ALLOWED_ORIGINS` | paydadosh.ru, localhost | Host / Origin checks |

Behind a reverse proxy: forward `/mcp` with `X-Real-IP`, no buffering and no caching. The registry entry is in
`server.json` (`ru.paydadosh/dictionary`).
