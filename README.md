# chatgpt-use

English | [한국어](README.ko.md)

Drive your **ChatGPT web subscription** as a coding-agent backend. No API key, no Codex billing.

> Unofficial browser automation, not an OpenAI-supported API. It drives a logged-in chatgpt.com tab
> through [`chrome-use`](https://github.com/leeguooooo/chrome-use).
>
> Fork of [`leeguooooo/chatgpt-use`](https://github.com/leeguooooo/chatgpt-use), maintained at
> [`giinii06/chatgpt-use`](https://github.com/giinii06/chatgpt-use).

![chatgpt-use](assets/hero.png)

Your Plus / Pro plan already includes a chat surface you've paid for. `chatgpt-use` drives that
logged-in conversation into your local machine: an agent hands work to ChatGPT, which can read and
edit your project files. Not the API, not the Codex bucket — quota you already bought.

## Quick start

You need Chrome signed in to chatgpt.com on a Plus/Pro plan, and
[`chrome-use`](https://github.com/leeguooooo/chrome-use) on `PATH`.

```bash
curl -fsSL https://raw.githubusercontent.com/leeguooooo/chrome-use/main/install.sh | sh
curl -fsSL https://raw.githubusercontent.com/giinii06/chatgpt-use/main/install.sh | sh
chatgpt-use ask "Explain this failure and how to fix it" --file build.log
```

```powershell
# Windows PowerShell
irm https://raw.githubusercontent.com/giinii06/chatgpt-use/main/install.ps1 | iex
```

Or from source: `cargo install --git https://github.com/giinii06/chatgpt-use`.

## Choose a mode

`chatgpt-use` is one engine — a channel to the ChatGPT web conversation — exposed three ways
depending on who's the brain.

| Mode | Command | Who thinks | Use it for |
|---|---|---|---|
| Sidekick | `chatgpt-use ask` | your agent | a second opinion, a review, a plan |
| Brain | `chatgpt-use run` | ChatGPT | a task that needs local tools, in a loop |
| Drop-in | `chatgpt-use serve` | Claude Code | ChatGPT as the model behind an Anthropic-compatible endpoint |

```bash
# Sidekick — one-shot, optional file context, no tools
chatgpt-use ask "Which of these two migrations is safer?" --file a.sql --file b.sql

# Structured delegation — ChatGPT returns a verdict packet
chatgpt-use ask "Review this diff" --mode review --file change.patch --json
chatgpt-use handoff plan.json --to codex          # dry-run unless --execute

# Brain — ChatGPT drives read/write/bash locally until done
chatgpt-use run "Add retries to the uploader and run the tests" --approve

# Drop-in — Claude Code keeps its loop; ChatGPT answers
chatgpt-use serve --port 8787
ANTHROPIC_BASE_URL=http://127.0.0.1:8787 claude
```

`ask` answers. `run` acts. `serve` plugs in. Shared flags: `--model`, `--profile`, `--session`,
`--project`; `--model` takes `instant|medium|high|"extra high"|pro`.

## Use it from a coding agent

Any agent that can run a shell command can use the CLI's `--json` envelope. Any MCP client can use
`chatgpt-use agent-mcp`, which exposes `ask`, `status`, `resume` and `cancel` over stdio and returns
the same envelope as `structuredContent`. It exposes no file or shell tools — that is
`chatgpt-use mcp`, which ChatGPT calls.

```bash
chatgpt-use ask --json --request-id review-42 "Review this diff" < change.patch
chatgpt-use status review-42     # never touches the browser
chatgpt-use resume review-42     # wait for the reply without resending
chatgpt-use cancel review-42
```

A ChatGPT turn can take minutes. Two timeouts must both allow it: the server's `--timeout` per turn
(default 300 s) and the agent's tool-call timeout. Set the server's first, then the agent's a little
higher, so the server answers with an `incomplete` envelope instead of the agent cutting the call.

| Agent | Register | Tool-call timeout |
|---|---|---|
| Claude Code | `claude mcp add chatgpt-use -- chatgpt-use agent-mcp --timeout 900` | `MCP_TOOL_TIMEOUT=960000` (ms) |
| Codex | `codex mcp add chatgpt-use -- chatgpt-use agent-mcp --timeout 900` | `tool_timeout_sec = 960` under `[mcp_servers.chatgpt-use]` |
| Pi ≥ 0.99 | `pi mcp add chatgpt-use -- chatgpt-use agent-mcp --timeout 900` | progress notifications reset the 60 s default |

The skill: `npx skills add giinii06/chatgpt-use`.

## Other commands

| Command | What it does |
|---|---|
| `work "<task>"` | closed loop: ChatGPT does the task through its MCP connector and reports back |
| `review` | code review that explores the repo in a throwaway worktree |
| `mcp` | local MCP server exposing project tools to ChatGPT through a tunnel |
| `init` | write `~/.chatgpt-use/auth.json` for `mcp` |
| `refresh` | re-register the connector after restarting `mcp` |
| `upgrade` | install the latest release and refresh the skill |

`mcp` and `agent-mcp` point in opposite directions: `mcp` is called **by** ChatGPT over a public
tunnel, `agent-mcp` is called **by** your agent over stdio.

## How it works

```
agent ──▶ chatgpt-use ──▶ chrome-use ──▶ logged-in chatgpt.com tab
              ▲                                    │
              └──── conversation record ◀──────────┘
```

Page-side work is all `chrome-use eval <js>`: fill `#prompt-textarea`, submit, poll.

The turn is finished when the server says so, not when the page stops changing. `chatgpt-use` reads
the conversation record (`/backend-api/conversation/<id>`) and walks the newest answer's parent
chain. Done means the newest final-channel message is complete, `end_turn` is set, and nothing newer
is still being written. DOM quiescence is an inference and was wrong in both directions: a mid-stream
pause read as an ending, a finished reply read as a timeout. The reply text also comes from the
record, not `innerText` — rendered markdown drops syntax (a 600-word reply lost 87 characters), and
a hidden regenerated tail above the visible answer used to hold turns open forever.

## Limitations and security

- **Unofficial.** ChatGPT's web UI changes without notice; when it does, this breaks until patched.
- **Long turns are slow.** A multi-minute turn is normal. Set both timeouts as described above.
- **`run --approve`** gates each local tool call. Without it, ChatGPT acts on your project directly.
- **`mcp` tunnels your files.** Default is read-only and sandboxed to `--cwd`. `--profile full` adds
  writes and a shell; `--permission-mode dangerous` removes command gating entirely. Only expose that
  on a trusted machine, always with a random `--token`.
- **Terms of service.** This drives your own account; use it the way you would use the web app.

## Documentation

- [`docs/architecture.html`](docs/architecture.html) — the full design
- [`docs/mcp-setup.html`](docs/mcp-setup.html) — connector setup, step by step
- [`docs/failure-semantics-corpus.md`](docs/failure-semantics-corpus.md) — measured failure cases
- [`SKILL.md`](SKILL.md) — the agent skill

## Credits and license

Stands on [`chrome-use`](https://github.com/leeguooooo/chrome-use) (browser automation),
[`chatgpt-imagegen`](https://github.com/leeguooooo/chatgpt-imagegen) (the web-driving playbook), and
[`cookie-use`](https://github.com/leeguooooo/cookie-use) (the CLI-on-chrome-use model). Idea seeded
by [@VincentLogic](https://x.com/VincentLogic/status/2066800292604026943).

Forked from [`leeguooooo/chatgpt-use`](https://github.com/leeguooooo/chatgpt-use). MIT, Copyright
(c) 2026 leeguooooo.
