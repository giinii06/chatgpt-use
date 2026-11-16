# chatgpt-use

[English](README.md) | 한국어

**ChatGPT 웹 구독**을 코딩 에이전트의 백엔드로 씁니다. API 키도, Codex 과금도 없습니다.

> OpenAI가 지원하는 API가 아니라 비공식 브라우저 자동화입니다. 로그인된 chatgpt.com 탭을
> [`chrome-use`](https://github.com/leeguooooo/chrome-use)로 조작합니다.
>
> [`leeguooooo/chatgpt-use`](https://github.com/leeguooooo/chatgpt-use)의 포크이며,
> [`giinii06/chatgpt-use`](https://github.com/giinii06/chatgpt-use)에서 관리합니다.

![chatgpt-use](assets/hero.png)

Plus / Pro 요금제에는 이미 돈을 낸 채팅 화면이 들어 있습니다. `chatgpt-use`는 그 로그인된 웹
대화를 로컬 머신에 연결합니다. 에이전트가 일을 ChatGPT에 넘기고, ChatGPT는 프로젝트 파일을 읽고
고칩니다. API도 Codex 버킷도 아닌, 이미 산 할당량으로 돌아갑니다.

## 빠른 시작

ChatGPT Plus/Pro로 로그인된 Chrome과 `PATH`에 있는
[`chrome-use`](https://github.com/leeguooooo/chrome-use)가 필요합니다.

```bash
curl -fsSL https://raw.githubusercontent.com/leeguooooo/chrome-use/main/install.sh | sh
curl -fsSL https://raw.githubusercontent.com/giinii06/chatgpt-use/main/install.sh | sh
chatgpt-use ask "이 실패 원인과 수정 방법을 알려줘" --file build.log
```

```powershell
# Windows PowerShell
irm https://raw.githubusercontent.com/giinii06/chatgpt-use/main/install.ps1 | iex
```

소스에서 설치: `cargo install --git https://github.com/giinii06/chatgpt-use`.

## 모드 선택

`chatgpt-use`는 ChatGPT 웹 대화로 가는 하나의 엔진이고, 누가 두뇌인지에 따라 세 가지로
노출됩니다.

| 모드 | 명령 | 두뇌 | 쓰임 |
|---|---|---|---|
| 사이드킥 | `chatgpt-use ask` | 내 에이전트 | 두 번째 의견, 리뷰, 계획 |
| 두뇌 | `chatgpt-use run` | ChatGPT | 로컬 도구가 필요한 작업을 루프로 |
| 드롭인 | `chatgpt-use serve` | Claude Code | Anthropic 호환 엔드포인트 뒤의 모델로 |

```bash
# 사이드킥 — 한 번 묻기, 파일 컨텍스트 선택, 도구 없음
chatgpt-use ask "두 마이그레이션 중 뭐가 더 안전해?" --file a.sql --file b.sql

# 구조화 위임 — ChatGPT가 판정 패킷을 돌려줌
chatgpt-use ask "이 diff를 리뷰해줘" --mode review --file change.patch --json
chatgpt-use handoff plan.json --to codex          # --execute 없으면 드라이런

# 두뇌 — ChatGPT가 로컬 read/write/bash를 돌려 작업을 끝냄
chatgpt-use run "업로더에 재시도를 넣고 테스트를 돌려" --approve

# 드롭인 — Claude Code는 자기 루프를 유지하고 ChatGPT가 답함
chatgpt-use serve --port 8787
ANTHROPIC_BASE_URL=http://127.0.0.1:8787 claude
```

`ask`는 답하고, `run`은 실행하고, `serve`는 끼워 넣습니다. 공통 플래그는 `--model`, `--profile`,
`--session`, `--project`이고, `--model`은 `instant|medium|high|"extra high"|pro`를 받습니다.

## 코딩 에이전트에서 쓰기

셸 명령을 돌릴 수 있는 에이전트는 CLI의 `--json` 봉투를 그대로 쓸 수 있습니다. MCP 클라이언트는
`chatgpt-use agent-mcp`를 쓰면 됩니다. stdio로 `ask`, `status`, `resume`, `cancel`을 노출하고
같은 봉투를 `structuredContent`로 돌려줍니다. 파일이나 셸 도구는 노출하지 않습니다. 그건
ChatGPT가 호출하는 `chatgpt-use mcp` 쪽입니다.

```bash
chatgpt-use ask --json --request-id review-42 "이 diff를 리뷰해줘" < change.patch
chatgpt-use status review-42     # 브라우저를 건드리지 않음
chatgpt-use resume review-42     # 다시 보내지 않고 응답만 기다림
chatgpt-use cancel review-42
```

ChatGPT 턴은 몇 분씩 걸릴 수 있습니다. 두 타임아웃이 모두 허용해야 합니다. 서버의 턴당
`--timeout`(기본 300초)과 에이전트의 도구 호출 타임아웃입니다. 서버 쪽을 먼저 올리고 에이전트
쪽을 조금 더 크게 두세요. 그래야 에이전트가 호출을 끊기 전에 서버가 `incomplete` 봉투로
답합니다.

| 에이전트 | 등록 | 도구 호출 타임아웃 |
|---|---|---|
| Claude Code | `claude mcp add chatgpt-use -- chatgpt-use agent-mcp --timeout 900` | `MCP_TOOL_TIMEOUT=960000` (ms) |
| Codex | `codex mcp add chatgpt-use -- chatgpt-use agent-mcp --timeout 900` | `[mcp_servers.chatgpt-use]` 아래 `tool_timeout_sec = 960` |
| Pi ≥ 0.99 | `pi mcp add chatgpt-use -- chatgpt-use agent-mcp --timeout 900` | 진행 알림이 60초 기본값을 갱신함 |

스킬: `npx skills add giinii06/chatgpt-use`.

## 그 밖의 명령

| 명령 | 하는 일 |
|---|---|
| `work "<작업>"` | 폐루프: ChatGPT가 MCP 커넥터로 작업을 직접 하고 보고 |
| `review` | 임시 워크트리에서 저장소를 탐색하는 코드 리뷰 |
| `mcp` | 터널을 통해 ChatGPT에 프로젝트 도구를 노출하는 로컬 MCP 서버 |
| `init` | `mcp`용 `~/.chatgpt-use/auth.json` 생성 |
| `refresh` | `mcp` 재시작 후 커넥터 재등록 |
| `upgrade` | 최신 릴리스 설치와 스킬 갱신 |

`mcp`와 `agent-mcp`는 방향이 반대입니다. `mcp`는 ChatGPT가 공개 터널 너머로 **호출하고**,
`agent-mcp`는 내 에이전트가 stdio로 **호출합니다**.

## 동작 방식

```
agent ──▶ chatgpt-use ──▶ chrome-use ──▶ 로그인된 chatgpt.com 탭
              ▲                                    │
              └──── 대화 record ◀──────────────────┘
```

페이지 쪽은 전부 `chrome-use eval <js>`입니다. `#prompt-textarea`를 채우고, 제출하고, 폴링합니다.
턴이 끝났는지는 화면이 멈췄는지가 아니라 서버가 말하는지로 판정합니다. `chatgpt-use`는 대화
record(`/backend-api/conversation/<id>`)에서 최신 답변의 부모 체인을 따라갑니다. 완료란 최신
final 메시지가 완성됐고 `end_turn`이 붙었고 그 위에 아직 쓰이는 것이 없다는 뜻입니다. DOM 정지는
추론이라 양방향으로 틀렸고, 답변 텍스트도 `innerText`가 아니라 record에서 옵니다. 렌더된
마크다운은 문법을 잃고(600단어 답변에서 87자 손실), hidden 재생성 꼬리는 턴을 영원히 열어 두곤
했습니다.

## 한계와 보안

- **비공식.** ChatGPT 웹 UI는 예고 없이 바뀌고, 그때는 패치 전까지 깨집니다.
- **긴 턴은 느립니다.** 몇 분짜리 턴은 정상입니다. 위에 적은 대로 두 타임아웃을 설정하세요.
- **`run --approve`** 는 로컬 도구 호출마다 승인을 겁니다. 없으면 ChatGPT가 프로젝트에 직접
  작업합니다.
- **`mcp`는 파일을 터널로 노출합니다.** 기본은 읽기 전용이고 `--cwd`로 샌드박스됩니다.
  `--profile full`은 쓰기와 셸을 더하고, `--permission-mode dangerous`는 명령 게이팅을 완전히
  없앱니다. 신뢰하는 머신에서만, 항상 무작위 `--token`과 함께 여세요.
- **약관.** 자기 계정을 조작합니다. 웹 앱을 쓰는 방식 그대로 쓰세요.

## 문서

- [`docs/architecture.html`](docs/architecture.html) — 전체 설계
- [`docs/mcp-setup.html`](docs/mcp-setup.html) — 커넥터 설정 단계별 안내
- [`docs/failure-semantics-corpus.md`](docs/failure-semantics-corpus.md) — 실측 실패 사례
- [`SKILL.md`](SKILL.md) — 에이전트 스킬

## 크레딧과 라이선스

[`chrome-use`](https://github.com/leeguooooo/chrome-use)(브라우저 자동화),
[`chatgpt-imagegen`](https://github.com/leeguooooo/chatgpt-imagegen)(웹 구동 플레이북),
[`cookie-use`](https://github.com/leeguooooo/cookie-use)(chrome-use 위 CLI 모델) 위에 서 있습니다.
아이디어는 [@VincentLogic](https://x.com/VincentLogic/status/2066800292604026943)에서 시작했습니다.

[`leeguooooo/chatgpt-use`](https://github.com/leeguooooo/chatgpt-use)에서 포크했습니다. MIT,
Copyright (c) 2026 leeguooooo.
