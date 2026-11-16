//! The shared engine: a `chrome-use`-driven channel to a logged-in ChatGPT web
//! conversation. Every mode goes through here. Port the proven web-driving
//! practices from chatgpt-imagegen (read its source at
//! /Users/leo/github.com/chatgpt-imagegen/chatgpt-imagegen):
//!   - locate the `chrome-use` binary; pick the browser (relay first, then a
//!     logged-in profile; honor `profile = auto|relay|"Profile N"`)
//!   - open chatgpt.com (optionally inside a ChatGPT Project), wait for the
//!     composer (`js_composer_helper!`: ProseMirror or the Lexical editor)
//!   - submit a message; poll page state until the stop/streaming control
//!     disappears (reply complete); detect the "Too many requests" dialog
//!   - read the newest assistant message text/markdown back out
//! All page interaction goes through `chrome-use eval <js>` returning JSON.
//!
//! Concurrency is 1 (it drives the one shared logged-in tab and the page rate-
//! limits hard) — serialize across processes like chatgpt-imagegen does.
//!
//! Owned by the CORE agent.

use anyhow::{anyhow, bail, Context, Result};
use std::fs::File;
use std::io::{Seek, Write};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

// Accepted chrome-use binary names, newest name first (mirrors chatgpt-imagegen).
const AB_BIN_CANDIDATES: &[&str] = &["chrome-use", "agent-browser", "agent-browser-stealth", "abs"];

const WEB_NEW_CHAT_URL: &str = "https://chatgpt.com/";
/// One shared chrome-use session name — deliberately NOT per-process.
///
/// A different session name is a different tab, so the old `chatgpt-use-<pid>`
/// default opened a fresh ChatGPT window for every invocation. That is not just
/// untidy: ChatGPT pushes toasts into EVERY open chatgpt.com tab (so a
/// document-wide read can pick up a sibling tab's content), and the account
/// rate-limits per account, not per tab. Both tools that drive this surface
/// (chatgpt-use and chatgpt-imagegen) use this name, so there is one window.
/// Pass `--session` to override when you deliberately want a separate tab.
const DEFAULT_SESSION: &str = "chatgpt-web";
const WEB_PROJECT_URL_TPL: &str = "https://chatgpt.com/g/{gizmo_id}/project";
// Reconnect target. The plain /c/<id> form resolves even for a chat filed under
// a Project — ChatGPT redirects it to /g/<gizmo>/c/<id> — so the conversation id
// alone is enough to find our way back.
const WEB_CONVO_URL_TPL: &str = "https://chatgpt.com/c/";

const RATE_LIMIT_MSG: &str =
    "chatgpt.com rate-limited this account ('Too many requests') — the page \
     surface needs a few minutes of quiet before it will serve again.";

/// A rate-limit error, recorded so every process on this machine backs off
/// (see `throttle`). Use it where the throttle is first seen, not for errors
/// that only restate a hit already recorded.
fn rate_limited(msg: impl Into<String>) -> anyhow::Error {
    crate::throttle::note_rate_limited();
    ChannelError::new(ErrorKind::RateLimited, msg).into()
}

/// Refuse while an account cooldown from an earlier hit is still running.
fn check_cooldown() -> Result<()> {
    match crate::throttle::cooldown_left() {
        Some(left) => {
            Err(ChannelError::new(ErrorKind::RateLimited, crate::throttle::refusal(left)).into())
        }
        None => Ok(()),
    }
}

/// Why a channel operation failed, in the terms a caller has to act on.
///
/// Most failures stay plain `anyhow` errors; only the ones that decide what a
/// caller should do next are typed. Retrieve one with [`channel_error`] —
/// it survives any `.context()` layered on top.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// No signed-in ChatGPT tab could be opened.
    LoginRequired,
    /// chatgpt.com's "Too many requests" throttle.
    RateLimited,
    /// The chrome-use session is wedged (see `Channel::connect`).
    SessionUnavailable,
    /// The turn never completed and the page is showing a dialog we do not
    /// recognise — a plan or usage-limit notice, say. Its text is in the
    /// message; we do not guess what it means.
    PageBlocked,
    /// Failed before the prompt reached ChatGPT. Safe to retry.
    NotSubmitted,
    /// Enter was pressed but no receipt appeared. The prompt may be on the
    /// server; resending it could post it twice.
    SubmitUnknown,
    /// Another run holds the ChatGPT window and `--busy fail` was asked for.
    Busy,
    /// The `--request-id` already names a request that may have reached
    /// ChatGPT; it is not sent again.
    Duplicate,
    /// Cancelled, and confirmed: either before anything was sent, or the
    /// conversation record shows the reply stopped (`finish_details`
    /// "interrupted").
    Cancelled,
    /// Stop was pressed but the record has not confirmed it; the reply may
    /// still be generating.
    CancelRequested,
    /// The prompt was sent but no complete reply arrived.
    Incomplete,
}

impl ErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorKind::LoginRequired => "login_required",
            ErrorKind::RateLimited => "rate_limited",
            ErrorKind::SessionUnavailable => "session_unavailable",
            ErrorKind::PageBlocked => "page_blocked",
            ErrorKind::NotSubmitted => "not_submitted",
            ErrorKind::SubmitUnknown => "submit_unknown",
            ErrorKind::Busy => "busy",
            ErrorKind::Duplicate => "duplicate_request",
            ErrorKind::Cancelled => "cancelled",
            ErrorKind::CancelRequested => "cancel_requested",
            ErrorKind::Incomplete => "incomplete",
        }
    }

    /// The caller-facing status this failure maps to. None of them is
    /// "completed": that status belongs to a reply that exists.
    pub fn status(self) -> &'static str {
        match self {
            ErrorKind::LoginRequired
            | ErrorKind::RateLimited
            | ErrorKind::SessionUnavailable
            | ErrorKind::PageBlocked => "unavailable",
            ErrorKind::NotSubmitted => "failed",
            ErrorKind::SubmitUnknown | ErrorKind::Incomplete => "incomplete",
            ErrorKind::Busy => "busy",
            ErrorKind::Duplicate => "duplicate",
            ErrorKind::Cancelled => "cancelled",
            ErrorKind::CancelRequested => "cancel_requested",
        }
    }
}

/// Whether the prompt of the failed turn reached ChatGPT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Submitted {
    No,
    Yes,
    Unknown,
}

impl Submitted {
    pub fn as_str(self) -> &'static str {
        match self {
            Submitted::No => "no",
            Submitted::Yes => "yes",
            Submitted::Unknown => "unknown",
        }
    }
}

#[derive(Debug)]
pub struct ChannelError {
    pub kind: ErrorKind,
    pub submitted: Submitted,
    message: String,
}

impl ChannelError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        ChannelError { kind, submitted: Submitted::No, message: message.into() }
    }

    pub fn with_submitted(mut self, submitted: Submitted) -> Self {
        self.submitted = submitted;
        self
    }
}

impl std::fmt::Display for ChannelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ChannelError {}

/// The typed cause of a channel failure, if it has one.
pub fn channel_error(e: &anyhow::Error) -> Option<&ChannelError> {
    e.downcast_ref::<ChannelError>()
        .or_else(|| e.chain().find_map(|c| c.downcast_ref::<ChannelError>()))
}

/// Type a failed turn by how far it got. A typed error keeps its kind but
/// takes its `submitted` from the turn, except `Unknown`, which only the
/// submit step can know and nothing later may downgrade.
fn classify(mut e: anyhow::Error, submitted: bool) -> anyhow::Error {
    let phase = if submitted { Submitted::Yes } else { Submitted::No };
    if let Some(ce) = e.downcast_mut::<ChannelError>() {
        if ce.submitted != Submitted::Unknown {
            ce.submitted = phase;
        }
        return e;
    }
    let (kind, label) = if submitted {
        (ErrorKind::Incomplete, "the prompt was sent but the reply did not complete")
    } else {
        (ErrorKind::NotSubmitted, "the prompt was not sent")
    };
    e.context(ChannelError::new(kind, label).with_submitted(phase))
}

/// Set by SIGTERM / SIGINT once `install_cancel_handler` has run. Every wait
/// in a turn watches it, so a cancel is honoured within about a second.
static CANCEL: std::sync::OnceLock<std::sync::Arc<std::sync::atomic::AtomicBool>> =
    std::sync::OnceLock::new();

/// Turn SIGTERM and SIGINT into a graceful cancel of the current request: stop
/// its reply and confirm it, instead of dying with the generation still
/// running. A second signal exits at once. Installed only by `ask
/// --request-id`, so every other command keeps the default behaviour.
pub fn install_cancel_handler() {
    #[cfg(unix)]
    {
        let flag = CANCEL
            .get_or_init(|| std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)))
            .clone();
        for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
            // Order matters: the conditional shutdown sees the flag the first
            // signal is about to set, so only a SECOND signal exits.
            let _ = signal_hook::flag::register_conditional_shutdown(sig, 130, flag.clone());
            let _ = signal_hook::flag::register(sig, flag.clone());
        }
    }
}

thread_local! {
    /// The cancel flag of the task running on this thread, if it has one.
    static TASK_CANCEL: std::cell::RefCell<Option<std::sync::Arc<std::sync::atomic::AtomicBool>>> =
        const { std::cell::RefCell::new(None) };
}

/// Run `f` with `flag` as this thread's cancel signal, beside the process-wide
/// one. A long-lived server (`agent-mcp`) gives each request its own flag, so
/// cancelling one stops only that request, and nothing carries over to the
/// next: the flag is dropped from the thread when `f` returns or panics.
pub fn with_cancel_flag<T>(
    flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
    f: impl FnOnce() -> T,
) -> T {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            TASK_CANCEL.with(|c| *c.borrow_mut() = None);
        }
    }
    TASK_CANCEL.with(|c| *c.borrow_mut() = Some(flag));
    let _reset = Reset;
    f()
}

fn cancel_requested() -> bool {
    use std::sync::atomic::Ordering::SeqCst;
    CANCEL.get().is_some_and(|f| f.load(SeqCst))
        || TASK_CANCEL.with(|c| c.borrow().as_ref().is_some_and(|f| f.load(SeqCst)))
}

/// Compare the composer's fingerprint with the message's. Fails closed: a
/// probe that errored or came back without a count and hash is not a pass,
/// or the one check that catches a truncated, polluted or scrambled prompt
/// would be skipped exactly when the page has changed under us.
fn check_fingerprint(got: Option<&serde_json::Value>, want_n: u64, want_h: u64) -> Result<()> {
    let n = got.and_then(|v| v.get("n")).and_then(|v| v.as_u64());
    let h = got.and_then(|v| v.get("h")).and_then(|v| v.as_u64());
    let (Some(n), Some(h)) = (n, h) else {
        bail!(
            "could not read the composer back to check it ({}) — refusing to submit a \
             prompt that was not verified",
            got.map_or("no reply".to_string(), |v| v.to_string())
        );
    };
    if n != want_n {
        bail!(
            "composer content doesn't match the message to send ({n} non-whitespace chars \
             present, {want_n} expected) — refusing to submit a truncated or polluted prompt"
        );
    }
    if h != want_h {
        bail!(
            "composer holds the right number of characters ({n}) but not in the right order \
             (fingerprint {h:#x}, expected {want_h:#x}) — refusing to submit a scrambled prompt"
        );
    }
    Ok(())
}

/// Whether a chrome-use failure says the session's tab is gone (closed,
/// navigated across processes, or lost by the relay) — recoverable by closing
/// the session and opening a fresh tab.
fn tab_gone(message: &str) -> bool {
    message.contains("driving is gone")
}

/// Polls in a row the turn-one count must stay down before the page counts
/// as replaced.
const UNPINNED_DIPS_BEFORE_LOST: u32 = 3;

/// Turn one, before ChatGPT has put a conversation id in the URL, has only one
/// sign that the tab now shows a different page: the user-turn count, which
/// rose when we submitted, falling back to the baseline. A single such read is
/// not enough — seen live, the poll that lands while a new chat moves from `/`
/// to `/c/<id>` re-renders the thread and reads low once, and treating that as
/// fatal threw away a turn that was completing normally. Only a dip that
/// persists for [`UNPINNED_DIPS_BEFORE_LOST`] polls counts; any other read
/// resets the count.
fn unpinned_page_lost(pinned: bool, users_now: Option<u64>, baseline: u64, dips: &mut u32) -> bool {
    if !pinned && users_now.is_some_and(|n| n <= baseline) {
        *dips += 1;
    } else {
        *dips = 0;
    }
    *dips >= UNPINNED_DIPS_BEFORE_LOST
}

/// The page's user turns at one moment: how many are rendered, and the stable
/// identities it exposes (see `js_turn_helpers!`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct UserTurns {
    count: u64,
    /// Identity of each rendered user turn.
    ids: Vec<String>,
    /// Every turn identity the page still holds, including virtualized turns
    /// whose contents are not rendered.
    known: Vec<String>,
}

impl UserTurns {
    fn from_json(v: &serde_json::Value) -> Option<Self> {
        let strs = |k: &str| -> Vec<String> {
            v.get(k)
                .and_then(|a| a.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect())
                .unwrap_or_default()
        };
        Some(UserTurns {
            count: v.get("count")?.as_u64()?,
            ids: strs("ids"),
            known: strs("known"),
        })
    }
}

/// Did a user turn appear in `now` that was not on the page at `base`?
///
/// This is the submit receipt. When the page exposes turn identities the answer
/// is "is some user turn's identity new", which a virtualized chat cannot fool:
/// an old turn scrolling back into view keeps its identity, and one scrolling
/// out does not hide a new one, whereas both move a plain count. If any
/// rendered turn lacks an identity (an older page), it falls back to the count
/// rising.
fn user_turn_rose(base: &UserTurns, now: &UserTurns) -> bool {
    let identified = |t: &UserTurns| t.ids.len() as u64 == t.count;
    if !identified(base) || !identified(now) {
        return now.count > base.count;
    }
    let seen: std::collections::HashSet<&str> =
        base.ids.iter().chain(&base.known).map(String::as_str).collect();
    now.ids.iter().any(|id| !seen.contains(id.as_str()))
}

/// Sleep that wakes early for a cancel.
fn nap(d: Duration) {
    let end = Instant::now() + d;
    while !cancel_requested() {
        let left = end.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return;
        }
        std::thread::sleep(left.min(Duration::from_millis(200)));
    }
}

/// JS regex literal for the "Too many requests" dialog, in each UI language
/// ChatGPT has been seen to render it in. An English-only test read a throttled
/// zh-CN page as healthy and kept polling into the throttle. Phrases taken from
/// miuuyy/codex-chatgpt-web's dialog matcher.
macro_rules! js_rate_limit_re {
    () => {
        r"/too many requests|requests too quickly|太多请求|太多要求|过于频繁|過於頻繁|リクエストが多すぎます|リクエストの頻度が高すぎます|요청이 너무 많습니다|너무 많은 요청|요청을 너무 빠르게/i"
    };
}

/// JS helpers that read conversation turns in both of ChatGPT's renderers.
///
/// The older one puts `data-message-author-role` on each message. The newer one
/// groups a user message and its reply under one `[data-turn-key]` element,
/// marking the parts `[data-user-message-bubble]` and
/// `[data-conversation-role="assistant"]`, and need not carry the author-role
/// attribute at all. Counting author roles alone would read that page as having
/// no turns, so no submit could ever be confirmed. Role elements inside a group
/// are excluded so a page that has both markings counts each turn once.
///
/// `__cguUserIds` collects stable turn identities for [`user_turn_rose`]: the
/// `data-turn-id` of each user turn, plus every `data-turn-id-container`. Long
/// chats are virtualized; an off-screen turn loses its contents but keeps its
/// container, so remounting it later must not look like a new turn.
macro_rules! js_turn_helpers {
    () => {
        r#"
  const __cguOuter = (els) => els.filter(e => !els.some(o => o !== e && o.contains(e)));
  const __cguUnits = (role, group) => __cguOuter([...document.querySelectorAll(
    `[data-message-author-role="${role}"], [data-content-search-unit-key$=":${role}"], ` +
    `[data-chatgpt-search-unit-key$=":${role}"], [data-turn-key]:has(${group}, ` +
    `[data-content-search-unit-key$=":${role}"], [data-chatgpt-search-unit-key$=":${role}"])`)]);
  const __cguUsers = () => __cguUnits('user', '[data-user-message-bubble]');
  const __cguAssistants = () => __cguUnits('assistant',
    '[data-conversation-role="assistant"], [data-chatgpt-agent-turn-start]');
  const __cguParts = (el) => {
    if (!el.matches('[data-turn-key]')) return [el];
    const own = __cguOuter([...el.querySelectorAll('[data-conversation-role="assistant"], ' +
      '[data-message-author-role="assistant"], [data-content-search-unit-key$=":assistant"], ' +
      '[data-chatgpt-search-unit-key$=":assistant"]')]);
    return own.length ? own : __cguOuter([...el.querySelectorAll('.markdown')]);
  };
  const __cguText = (el) => el ? __cguParts(el)
    .map(e => (e.innerText || e.textContent || '').trim()).filter(Boolean).join('\n\n') : '';
  const __cguReplyText = () => {
    const a = __cguAssistants();
    const last = a[a.length - 1];
    if (!last) return '';
    const turn = (e) => { const t = e.closest('[data-turn-id]'); return t && t.getAttribute('data-turn-id'); };
    const users = __cguUsers();
    const lastUser = users[users.length - 1];
    if (!lastUser) return __cguText(last);
    // Only what follows the latest question is its reply. A grouped turn is
    // both its user and its assistant unit, so it counts as following itself.
    const after = a.filter(e => e === lastUser ||
      !(e.compareDocumentPosition(lastUser) & Node.DOCUMENT_POSITION_FOLLOWING));
    const id = turn(last);
    const tail = id ? after.filter(e => turn(e) === id) : after;
    return tail.map(__cguText).filter(Boolean).join('\n\n');
  };
  const __cguUserIds = () => {
    const ids = [];
    const known = [...document.querySelectorAll('[data-turn-id-container]')]
      .map(e => e.getAttribute('data-turn-id-container')).filter(Boolean);
    for (const u of __cguUsers()) {
      const key = u.getAttribute('data-turn-key');
      if (key) { ids.push('group:' + key); continue; }
      const unit = u.getAttribute('data-content-search-unit-key') || u.getAttribute('data-chatgpt-search-unit-key');
      if (unit) { ids.push('unit:' + unit); continue; }
      const t = u.closest('[data-turn-id]');
      const id = t && t.getAttribute('data-turn-id');
      if (id) ids.push(id);
    }
    for (const g of document.querySelectorAll('[data-turn-key]')) known.push('group:' + g.getAttribute('data-turn-key'));
    return {ids, known};
  };
"#
    };
}

/// JS helper `__cguComposer()`: the one composer the user would type into, or
/// null.
///
/// ChatGPT's editor has two forms in the wild: the ProseMirror box with id
/// `#prompt-textarea`, and the newer Lexical rich editor, which need not carry
/// that id at all (selectors as recorded by miuuyy/codex-chatgpt-web
/// `chatgpt-session.ts` and totec448-spec/chat-on-steroids `chatgpt-dom.js`,
/// both early October 2026). Looking only for the id read a page with the new
/// editor as having no composer, and every run failed before sending.
///
/// The app also keeps earlier pages mounted but hidden
/// (`[data-app-shell-page-surface]` with `display: none`), each with its own
/// editor, so the visible one is chosen, never the first match. Nested matches
/// resolve to the innermost editable node. More than one visible candidate is
/// ambiguous and reads as none: typing into the wrong box is worse than failing.
///
/// The chosen node is stamped `data-cgu-composer`, so a real-input click can
/// target it by selector (`[data-cgu-composer]`).
macro_rules! js_composer_helper {
    () => {
        r#"
  const __cguComposer = () => {
    const hidden = (n) => {
      for (let p = n.closest('[data-app-shell-page-surface]'); p;
           p = p.parentElement && p.parentElement.closest('[data-app-shell-page-surface]')) {
        if (getComputedStyle(p).display === 'none') return true;
      }
      return !!n.closest('[hidden], [aria-hidden="true"], [inert]');
    };
    const all = [...document.querySelectorAll(
      '#prompt-textarea, [data-testid="prompt-textarea"], ' +
      '[contenteditable="true"][data-lexical-editor="true"], ' +
      'form[data-chatgpt-composer] [contenteditable="true"][role="textbox"], ' +
      'form [data-composer-markdown][contenteditable="true"][role="textbox"]')]
      .filter(n => !hidden(n) && !n.closest('[data-turn-key], [data-message-author-role], .markdown'));
    const live = all.filter(n => !all.some(o => o !== n && n.contains(o)));
    for (const n of document.querySelectorAll('[data-cgu-composer]')) {
      if (!(live.length === 1 && live[0] === n)) n.removeAttribute('data-cgu-composer');
    }
    if (live.length !== 1) return null;
    live[0].setAttribute('data-cgu-composer', '1');
    return live[0];
  };
"#
    };
}

/// JS string literal: the nodes ChatGPT puts in the composer for an
/// @-mentioned app (as recorded by miuuyy/codex-chatgpt-web, Oct 2026).
macro_rules! js_pill_selector {
    () => {
        r#"'[data-id^="plugin:"][data-keyword], [app-mention-path^="app://"][app-mention-display-name][contenteditable="false"]'"#
    };
}

/// JS: the @-mention menu's rows and whether exactly one is the app `name`.
/// A row's title is its first line; a trailing "DEV" badge (a developer-mode
/// app) is allowed. Returns {titles, count, highlighted}.
fn js_mention_menu(name: &str) -> String {
    let n = serde_json::to_string(name).unwrap_or_else(|_| "\"\"".to_string());
    format!(
        r#"(() => {{
  const name = {n};
  const shown = (r) => !r.closest('[hidden], [aria-hidden="true"]') && getComputedStyle(r).display !== 'none';
  // `.__menu-item` is also the sidebar's class ("New chat", "Search", the
  // chat history — seen live), so a row only counts inside an open popup,
  // never inside navigation.
  const POPUP = '[data-mention-list-scroll-area], [role="listbox"], [role="menu"], [data-radix-popper-content-wrapper]';
  const rows = [...document.querySelectorAll(
    '.__menu-item[tabindex="0"], [data-mention-list-scroll-area] button[data-list-navigation-item="true"]')]
    .filter(r => shown(r) && r.closest(POPUP) && !r.closest('nav, aside, [data-testid*="sidebar"], #stage-slideover-sidebar'));
  const titles = rows.map(r => ((r.innerText || r.textContent || '').split('\n')[0] || '')
    .replace(/\s+/g, ' ').trim());
  const isName = (t) => t === name || (t.startsWith(name) && /^\s*DEV$/.test(t.slice(name.length)));
  const hits = rows.filter((r, i) => isName(titles[i]));
  const h = hits.length === 1 ? hits[0] : null;
  return JSON.stringify({{titles, count: hits.length,
    highlighted: !!h && (h.hasAttribute('data-highlighted') || h.getAttribute('aria-current') === 'true')}});
}})()"#
    )
}

/// JS: is the app `name` mentioned (a pill) in the composer? {ok, pills}.
fn js_connector_pill(name: &str) -> String {
    let n = serde_json::to_string(name).unwrap_or_else(|_| "\"\"".to_string());
    format!(
        r#"(() => {{{helper}
  const name = {n};
  const c = __cguComposer();
  if (!c) return JSON.stringify({{ok: false, pills: []}});
  const scope = c.closest('form') || c;
  const pills = [...scope.querySelectorAll({pills})]
    .map(p => p.getAttribute('data-keyword') || p.getAttribute('app-mention-display-name') || '');
  return JSON.stringify({{ok: pills.includes(name), pills}});
}})()"#,
        helper = js_composer_helper!(),
        pills = js_pill_selector!(),
    )
}

// JS: press the stop button, if the page is generating.
/// The send-button fallback in `submit`. A selector list matches in DOM order,
/// not list order, so the exact testids/ids stay page-wide while the loose
/// `*=send` / aria-label alternatives are scoped to the form holding the
/// composer we stamped (`data-cgu-composer`); otherwise any other button on the
/// page whose label contains "Send" could be clicked instead.
const SEND_BUTTON_SELECTOR: &str = concat!(
    r#"button[data-testid="send-button"], "#,
    r#"button[data-testid="fruitjuice-send-button"], "#,
    r#"button#composer-submit-button, "#,
    r#"form:has([data-cgu-composer]) button[data-testid*="send"], "#,
    r#"form:has([data-cgu-composer]) button[aria-label*="Send" i], "#,
    r#"form:has([data-cgu-composer]) button[aria-label*="发送"], "#,
    r#"form[data-chatgpt-composer] button[type="submit"]"#,
);

/// Dispatch a synthetic Enter on the stamped composer (see `submit`).
const JS_DISPATCH_ENTER: &str = r#"(() => {
  const el = document.querySelector('[data-cgu-composer]') || document.querySelector('#prompt-textarea');
  if (!el) return { ok: false };
  el.focus();
  for (const type of ['keydown', 'keypress', 'keyup']) {
    el.dispatchEvent(new KeyboardEvent(type, { key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true, cancelable: true }));
  }
  return { ok: true };
})()"#;

const JS_CLICK_STOP: &str = r#"(() => {
  const b = document.querySelector('button[data-testid="stop-button"], button[data-testid="composer-stop-button"], ' +
    'form button[type="button"][aria-label="Stop"], button[aria-label="Stop streaming"], button[aria-label="Stop generating"]');
  if (b) b.click();
  return JSON.stringify({clicked: !!b});
})()"#;

// JS: is this the signed-out page? Its login/sign-up buttons, or an auth URL.
// The button test ids are a best guess, not yet checked against a live
// signed-out page. If they are wrong, the error is the safe one: a real
// signed-out page reads as session_unavailable, never a false login_required.
const JS_WANTS_LOGIN: &str = r#"(() => JSON.stringify({login:
  /\/auth\/|auth\.openai\.com/.test(location.href) ||
  !!document.querySelector('[data-testid="login-button"],[data-testid="signup-button"]'),
  path: location.pathname, title: document.title.slice(0, 80)}))()"#;

// JS: text of a visible dialog, if any. Read only once a turn has stalled, to
// report WHAT is blocking it rather than guess (a usage-limit notice, say).
const JS_BLOCKING_DIALOG: &str = r#"(() => {
  const d = [...document.querySelectorAll('[role="dialog"],[role="alertdialog"]')]
    .find(x => x.getClientRects().length > 0);
  return JSON.stringify({text: d ? (d.innerText || '').trim().slice(0, 400) : ''});
})()"#;

// JS: poll composer presence + rate-limit dialog (mirrors _JS_COMPOSER in chatgpt-imagegen).
const JS_COMPOSER: &str = concat!(
    "(() => {",
    js_composer_helper!(),
    r#"
  const dlg = [...document.querySelectorAll('[role="dialog"]')]
    .map(d => d.textContent || '').join(' ');
  return JSON.stringify({
    composer: !!__cguComposer(),
    limited: "#,
    js_rate_limit_re!(),
    r#".test(dlg),
  });
})()"#
);

// JS: poll generation/reply state: stop button present? newest assistant text?
// rate-limited? Mirrors _JS_STATE in chatgpt-imagegen but without image scraping.
const JS_STATE: &str = concat!(
    "(() => {",
    js_turn_helpers!(),
    r#"
  const stop = !!document.querySelector(
    'button[data-testid="stop-button"], button[data-testid="composer-stop-button"], button[aria-label*="Stop" i]'
  );
  const a = __cguAssistants();
  const dlg = [...document.querySelectorAll('[role="dialog"]')]
    .map(d => d.textContent || '').join(' ');
  // A connector turn is multi-step: ChatGPT shows "Calling tool" / "Searching" /
  // "Running…" indicators while a tool runs (a build/test can take MINUTES). Treat
  // the turn as still in progress while any such active-tool marker is present, so
  // we don't scrape an intermediate ("I'll check next…") message instead of the
  // final report, and so we don't give up mid-build. Match PRESENT-tense action
  // verbs only — never past-tense "Thought for Xs" / "Worked for Xs", which persist
  // as static disclosures AFTER the turn ends and would wedge us as forever-active.
  // IMPORTANT: only scan UI chips (buttons), NOT prose. The progress indicator
  // ChatGPT shows while a connector tool runs is a button/disclosure whose text
  // starts with a present-tense action verb ("Running…", "Searching…"). Scanning
  // <div>/<span> too would also match the assistant's OWN words ("Running cargo
  // test…", "Reading Cargo.toml…") in the finished answer — which kept the turn
  // "active" forever and never settled. Verbs are present-progressive only; never
  // past-tense ("Ran"/"Searched"/"Thought for Xs"), which persist after the turn.
  const ACTIVE = /^(calling|searching|running|using|analyzing|analysing|executing|fetching|connecting|generating|working on)\b/i;
  const tool_active = [...document.querySelectorAll('button, [role="button"]')]
    .filter(b => !b.closest('[data-message-author-role], [data-conversation-role]')) // exclude in-message buttons
    .some(b => {
      const t = (b.textContent || '').trim();
      return t.length > 0 && t.length <= 24 && ACTIVE.test(t);
    });
  const cm = location.pathname.match(/\/c\/([0-9a-f-]{36})/i);
  return JSON.stringify({
    stop,
    tool_active,
    convo: cm ? cm[1] : "",
    // ChatGPT's own chrome. A scrape that equals it is the page title, not a
    // reply (see `usable_reply`).
    title: (document.title || '').slice(0, 200),
    user_count: __cguUsers().length,
    assistant_count: a.length,
    limited: "#,
    js_rate_limit_re!(),
    r#".test(dlg),
    atext: __cguReplyText()
  });
})()"#
);

// JS: scrape the full innerText of the last assistant message.
const JS_LAST_ASSISTANT: &str = concat!(
    "(() => {",
    js_turn_helpers!(),
    r#"
  return JSON.stringify(__cguReplyText());
})()"#
);

// JS: the conversation UUID this tab is currently showing, or "" on a
// not-yet-persisted new chat (the id only materializes after the first turn).
const JS_CONVO_ID: &str = r#"(() => {
  const m = location.pathname.match(/\/c\/([0-9a-f-]{36})/i);
  return JSON.stringify(m ? m[1] : "");
})()"#;

// JS: how many USER turns are rendered. ChatGPT renders the user bubble
// optimistically the instant a submit is accepted, so a rise in this count is
// authoritative, idempotent evidence that THIS turn was submitted — unlike
// "is the composer empty?", which races React's clear and misreads in both
// directions.
const JS_ASSISTANT_COUNT: &str = concat!(
    "(() => {",
    js_turn_helpers!(),
    r#"
  return JSON.stringify(__cguAssistants().length);
})()"#
);

// JS: the user turns as a [`UserTurns`] snapshot — count plus identities.
const JS_USER_TURNS: &str = concat!(
    "(() => {",
    js_turn_helpers!(),
    r#"
  const {ids, known} = __cguUserIds();
  return JSON.stringify({count: __cguUsers().length, ids, known});
})()"#
);

// JS: empty the composer, so a leftover fragment from an aborted turn can't be
// prepended to the next message.
const JS_CLEAR_COMPOSER: &str = concat!(
    "(() => {",
    js_composer_helper!(),
    r#"
  const c = __cguComposer();
  if (!c) return JSON.stringify({ok: false});
  c.focus();
  document.execCommand('selectAll');
  document.execCommand('delete');
  return JSON.stringify({ok: true});
})()"#
);

// JS: dismiss a blocking dialog (the rate-limit notice has a "Got it" button).
// Leaving it up keeps the composer unusable even after the throttle lifts.
const JS_DISMISS_DIALOG: &str = concat!(
    r#"(() => {
  const dlg = [...document.querySelectorAll('[role="dialog"]')]
    .find(d => "#,
    js_rate_limit_re!(),
    r#".test(d.textContent || ''));
  if (!dlg) return JSON.stringify({ok: false});
  const btn = [...dlg.querySelectorAll('button')]
    .find(b => /^(got it|ok|dismiss|close|知道了|了解|关闭|關閉|확인|알겠습니다|閉じる)$/i.test((b.textContent || '').trim()));
  if (btn) { btn.click(); return JSON.stringify({ok: true}); }
  return JSON.stringify({ok: false});
})()"#
);

// JS: fingerprint the composer's contents — the non-whitespace character COUNT
// and an order-sensitive HASH of those characters.
//
// Whitespace is excluded because ProseMirror renders each line as its own block,
// so innerText comes back with blank lines between them and a raw length would
// never match. The whitespace set is written out explicitly rather than using
// `\s`, because JavaScript's `\s` and Rust's `char::is_whitespace` disagree on a
// few code points (U+0085 is whitespace only to Rust, U+FEFF only to JS) and a
// single such character in a 30 KB prompt would fail the check for no reason.
//
// The hash is what makes this worth doing. A count alone catches truncation and
// pollution but is blind to REORDERING, and reordering is a real failure mode
// here: chrome-use's `keyboard inserttext` interleaves the tail of one chunk
// with the head of the next while preserving total length (leeguooooo/chrome-use#301,
// reproduced against this very composer). A payload can therefore arrive
// complete, correctly sized, and scrambled. FNV-1a over UTF-16 code units, which
// both sides can compute identically.
const JS_COMPOSER_FINGERPRINT: &str = concat!(
    "(() => {",
    js_composer_helper!(),
    "\n  const PILLS = ",
    js_pill_selector!(),
    ";",
    r#"
  const c = __cguComposer();
  // Connector pills (an @-mentioned app) are not message text.
  let t = '';
  if (c) {
    const k = c.cloneNode(true);
    k.querySelectorAll(PILLS).forEach(n => n.remove());
    t = k.innerText || k.textContent || '';
  }
  const isWs = (u) =>
    (u >= 0x09 && u <= 0x0d) || u === 0x20 || u === 0x85 || u === 0xa0 ||
    u === 0x1680 || (u >= 0x2000 && u <= 0x200a) || u === 0x2028 ||
    u === 0x2029 || u === 0x202f || u === 0x205f || u === 0x3000 || u === 0xfeff;
  let n = 0;
  let h = 0x811c9dc5;
  for (let i = 0; i < t.length; i++) {
    const u = t.charCodeAt(i);
    if (isWs(u)) continue;
    n++;
    h = (h ^ (u & 0xff)) >>> 0;
    h = Math.imul(h, 0x01000193) >>> 0;
    h = (h ^ (u >>> 8)) >>> 0;
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return JSON.stringify({n: n, h: h});
})()"#
);

/// Chips the composer renders for files it will send. Deliberately broad: the
/// app has renamed these test ids before, and a missed chip must read as "not
/// confirmed" (a refusal), never as "attached" (a silent send without them).
macro_rules! attachment_selector {
    () => {
        r#"[data-testid*="attachment" i], [data-testid*="file-chip" i], [class*="attachment" i]"#
    };
}
#[cfg(test)]
const ATTACHMENT_SELECTOR: &str = attachment_selector!();

const FILE_INPUT_SELECTOR: &str = "[data-cgu-file-input]";

/// JS: stamp the composer form's file input, and count the chips already there.
/// A stamped input is the only handle `chrome-use upload` needs; the stamp is
/// cleared first so a stale one from a previous turn cannot be targeted.
const JS_FILE_INPUT: &str = concat!(
    "(() => {",
    js_composer_helper!(),
    r#"
  const c = __cguComposer();
  const form = (c && c.closest('form')) || document;
  for (const n of document.querySelectorAll('[data-cgu-file-input]')) {
    n.removeAttribute('data-cgu-file-input');
  }
  const input = form.querySelector('input[type="file"]') ||
    document.querySelector('input[type="file"]');
  if (!input) return JSON.stringify({ok: false, existing: 0});
  input.setAttribute('data-cgu-file-input', '1');
  const chips = form.querySelectorAll('"#,
    attachment_selector!(),
    r#"');
  return JSON.stringify({ok: true, existing: chips.length});
})()"#
);

/// JS: how many attachment chips the composer form holds now.
const JS_ATTACHMENT_CHIPS: &str = concat!(
    "(() => {",
    js_composer_helper!(),
    r#"
  const c = __cguComposer();
  const form = (c && c.closest('form')) || document;
  const chips = form.querySelectorAll('"#,
    attachment_selector!(),
    r#"');
  return JSON.stringify({chips: chips.length});
})()"#
);

/// JS: insert `text` at the caret via `execCommand('insertText')`.
///
/// This is deliberately NOT `keyboard type`. A "\n" typed into ProseMirror is an
/// Enter — i.e. a SUBMIT — so typing any multi-line message (every `run`/`serve`
/// system prompt) chopped it at each newline and fired the pieces off as many
/// separate chat messages, which ChatGPT then answered as fragments. Verified
/// live: `keyboard type "A\nB"` submits "A" and leaves "B" in the box.
///
/// `insertText` treats "\n" as literal text while still firing the real
/// beforeinput/input events ProseMirror and React need, so the send button stays
/// bound to the live content (which is why `fill` was avoided in the first place).
fn js_insert_text(text: &str) -> String {
    let t = serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_string());
    format!(
        r#"(() => {{{helper}
  const c = __cguComposer();
  if (!c) return JSON.stringify({{ok: false, error: 'composer not found'}});
  c.focus();
  const ok = document.execCommand('insertText', false, {t});
  return JSON.stringify({{ok}});
}})()"#,
        helper = js_composer_helper!()
    )
}

/// A JS prelude every backend call shares: fetch the page's bearer token ONCE
/// and keep it on `window` until it is close to expiring.
///
/// `/api/auth/session` was being re-fetched by every single backend call. The
/// completion check alone runs every ~20s, so a five-minute turn spent fifteen
/// requests re-asking for a token that had not changed. The throttle that keeps
/// biting this account counts requests, so that is fifteen requests of pure
/// waste per turn, on top of whatever the call actually needed.
///
/// Cached against the session's own `expires`, with a minute of headroom, and
/// re-fetched on any failure — a stale token is worse than an extra request.
const JS_TOKEN_PRELUDE: &str = r#"
const __cguToken = async () => {
  try {
    const now = Date.now();
    const c = window.__cguTok;
    if (c && c.token && c.until > now) return c.token;
    const s = await fetch('/api/auth/session', {credentials: 'include'}).then(r => r.json());
    if (!s || !s.accessToken) return null;
    // `expires` on this payload is the SESSION's lifetime — observed 90 days —
    // not the access token's, so honouring it would cache a bearer far past its
    // real validity and turn every later call into a 401. Cap the cache at five
    // minutes: still collapses the ~15 fetches a long turn used to make into
    // one or two, without betting on a token staying good.
    const exp = Date.parse(s.expires || '');
    const cap = now + 300000;
    const until = Number.isFinite(exp) ? Math.min(exp - 60000, cap) : cap;
    window.__cguTok = {token: s.accessToken, until: until};
    return s.accessToken;
  } catch (e) {
    return null;
  }
};
"#;

/// JS: project the conversation record for the current turn.
///
/// This probe used to decide completion itself: it walked `current_node`'s
/// parent chain and returned the first assistant message with text, with
/// `done` taken from that message's `end_turn`. On a multi-step turn the
/// newest message is often an intermediate step, and a per-message `end_turn`
/// says that STEP closed, not that the turn did — so the caller was told an
/// interim message was the finished answer. Probed offline against fixtures:
/// a visible progress message and a thinking step each came back `done: true`.
///
/// The walk stays here because it needs the payload; the DECISION moved to
/// [`judge_record`], where it is pure and testable. Every field the judgement
/// needs is projected, including `channel` and `status`, which is what tells a
/// final answer from a commentary step.
///
/// The mapping is a TREE — editing a prompt or regenerating forks it and the
/// abandoned branches stay in the payload — so the live thread is `current_node`'s
/// parent chain, not `Object.values(mapping)`.
fn js_server_final(convo_id: &str) -> String {
    let c = serde_json::to_string(convo_id).unwrap_or_else(|_| "\"\"".to_string());
    format!(
        r#"(async () => {{
  {prelude}
  try {{
    const tok = await __cguToken();
    if (!tok) return JSON.stringify({{ok: false, error: 'not signed in'}});
    const r = await fetch('/backend-api/conversation/' + {c}, {{
      credentials: 'include',
      headers: {{Authorization: 'Bearer ' + tok}},
    }});
    if (!r.ok) return JSON.stringify({{ok: false, error: 'HTTP ' + r.status}});
    const j = await r.json();
    const map = j.mapping || {{}};
    const chain = [];
    const seen = {{}};
    let cur = j.current_node;
    while (cur && map[cur] && !seen[cur]) {{ seen[cur] = 1; chain.push(map[cur]); cur = map[cur].parent; }}
    const textOf = (c2) => {{
      if (!c2) return '';
      if (Array.isArray(c2.parts)) {{
        return c2.parts.map(p => typeof p === 'string' ? p : (p && p.text) || '')
          .filter(Boolean).join('\n').trim();
      }}
      return typeof c2.content === 'string' ? c2.content.trim() : '';
    }};
    // chain is leaf -> root. Stop AT the latest user message: past it lies the
    // previous turn's reply, which is closed and has text.
    const nodes = [];
    let user = null;
    for (const n of chain) {{
      const m = n.message;
      if (!m || !m.author) {{ nodes.push({{id: n.id || '', role: ''}}); continue; }}
      if (m.author.role === 'user') {{ user = textOf(m.content); break; }}
      const c2 = m.content || {{}};
      const ct = c2.content_type || '';
      const fd = (m.metadata && m.metadata.finish_details) || {{}};
      nodes.push({{
        id: n.id || m.id || '',
        role: m.author.role || '',
        content_type: ct,
        channel: m.channel || '',
        recipient: m.recipient || '',
        weight: typeof m.weight === 'number' ? m.weight : 1,
        end_turn: m.end_turn === true,
        status: m.status || '',
        node_status: n.status || '',
        is_complete: m.is_complete !== false,
        hidden: !!(m.metadata && m.metadata.is_visually_hidden_from_conversation),
        finish: fd.type || '',
        reason: fd.reason || '',
        // Only visible prose is carried: reasoning and tool payloads can be
        // enormous and are never the answer.
        text: (ct === '' || ct === 'text' || ct === 'multimodal_text') ? textOf(c2) : '',
      }});
    }}
    return JSON.stringify({{ok: true,
      conversation_id: j.conversation_id || '',
      user_text: user,
      nodes: nodes}});
  }} catch (e) {{
    return JSON.stringify({{ok: false, error: String(e)}});
  }}
}})()"#,
        prelude = JS_TOKEN_PRELUDE
    )
}

/// JS: move an existing conversation into a Project, server-side.
///
/// The escape hatch for when ChatGPT's project PAGE will not render — observed
/// account-wide for hours, every project showing only a "Try again" button while
/// the backend API kept answering normally. The UI being broken is not a reason
/// for `--project` to silently stop working: the conversation can be created in
/// a plain chat and filed afterwards, which is what this does.
fn js_file_into_project(convo_id: &str, gizmo_id: &str) -> String {
    let c = serde_json::to_string(convo_id).unwrap_or_else(|_| "\"\"".to_string());
    let g = serde_json::to_string(gizmo_id).unwrap_or_else(|_| "\"\"".to_string());
    format!(
        r#"(async () => {{
  {prelude}
  try {{
    const tok = await __cguToken();
    if (!tok) return JSON.stringify({{ok: false, error: 'not signed in'}});
    const r = await fetch('/backend-api/conversation/' + {c}, {{
      method: 'PATCH',
      credentials: 'include',
      headers: {{Authorization: 'Bearer ' + tok, 'Content-Type': 'application/json'}},
      body: JSON.stringify({{gizmo_id: {g}}}),
    }});
    if (!r.ok) return JSON.stringify({{ok: false, error: 'HTTP ' + r.status}});
    return JSON.stringify({{ok: true}});
  }} catch (e) {{
    return JSON.stringify({{ok: false, error: String(e)}});
  }}
}})()"#,
        prelude = JS_TOKEN_PRELUDE
    )
}

/// The thinking-effort levels, in slider order. The INDEX is the contract with
/// the page (`aria-valuenow`); the names here are only what a caller types.
const LEVEL_ORDER: &[&str] = &["instant", "medium", "high", "extra high", "pro"];

/// `--model current` (or `default`): use whatever the account is set to and
/// leave the picker alone. An escape hatch for when the picker has changed
/// again and a run does not need a particular model.
pub fn keeps_current_model(model: &str) -> bool {
    matches!(model.trim().to_lowercase().as_str(), "current" | "default")
}

/// Map a `--model` value to a slider index, or `None` if it names a model
/// family rather than an effort level.
fn level_index(want: &str) -> Option<usize> {
    let norm = want.trim().to_lowercase().replace(['-', '_'], " ");
    let norm = norm.split_whitespace().collect::<Vec<_>>().join(" ");
    if norm == "extrahigh" {
        return Some(3);
    }
    LEVEL_ORDER.iter().position(|l| *l == norm)
}

// JS: start a fresh chat WITHOUT reloading the page.
//
// A full navigation to https://chatgpt.com/ costs 45 backend-api requests on
// this account — the SPA re-boots and re-fetches every project's metadata and
// conversation list (9 projects here, so 18 requests before anything else).
// Clicking the app's own "New chat" control does the same job client-side for
// exactly ONE request (/backend-api/conversation/init). Measured, both numbers.
//
// That difference is the whole rate-limit story: the throttle counts requests,
// not messages, so reloading the page once per invocation was costing 45x what
// the work actually needed.
const JS_NEW_CHAT_IN_PLACE: &str = r#"(() => {
  if (!/(^|\.)chatgpt\.com$/.test(location.hostname)) {
    return JSON.stringify({ok: false, error: 'not on chatgpt.com'});
  }
  const cands = [...document.querySelectorAll('a, button, [role="button"]')];
  const hit = cands.find(el => {
    const al = (el.getAttribute('aria-label') || '').trim();
    const tx = (el.textContent || '').trim();
    return /^new chat$/i.test(al) || /^new chat$/i.test(tx);
  });
  if (!hit) return JSON.stringify({ok: false, error: 'no new-chat control'});
  hit.click();
  return JSON.stringify({ok: true});
})()"#;

/// JS: switch to an already-open conversation through the SPA's own sidebar
/// link, rather than navigating.
///
/// 9 backend-api requests instead of 45. The pinned conversation is always a
/// recent one, so it is in the sidebar; anything older falls back to a real
/// navigation, which is still correct, just costlier.
fn js_open_convo_in_place(convo_id: &str) -> String {
    let c = serde_json::to_string(convo_id).unwrap_or_else(|_| "\"\"".to_string());
    format!(
        r#"(() => {{
  if (!/(^|\.)chatgpt\.com$/.test(location.hostname)) {{
    return JSON.stringify({{ok: false, error: 'not on chatgpt.com'}});
  }}
  const want = {c};
  const link = [...document.querySelectorAll('a[href*="/c/"]')]
    .find(a => (a.getAttribute('href') || '').includes(want));
  if (!link) return JSON.stringify({{ok: false, error: 'conversation not in the sidebar'}});
  link.click();
  return JSON.stringify({{ok: true}});
}})()"#
    )
}

/// JS: open a Project through its sidebar link instead of navigating to it.
fn js_open_project_in_place(gizmo_id: &str) -> String {
    let g = serde_json::to_string(gizmo_id).unwrap_or_else(|_| "\"\"".to_string());
    format!(
        r#"(() => {{
  if (!/(^|\.)chatgpt\.com$/.test(location.hostname)) {{
    return JSON.stringify({{ok: false, error: 'not on chatgpt.com'}});
  }}
  const want = {g};
  // Only the link to the project PAGE. The sidebar also lists conversations
  // inside the project, whose hrefs carry the same gizmo id; clicking one opens
  // that old conversation, and the readiness check (URL contains the gizmo id)
  // would pass on it, so the new prompt would land in the old chat.
  const link = [...document.querySelectorAll('a[href*="/g/"]')]
    .find(a => {{
      const href = a.getAttribute('href') || '';
      return href.includes(want) && /\/project\/?$/.test(href);
    }});
  if (!link) return JSON.stringify({{ok: false, error: 'project not in the sidebar'}});
  link.click();
  return JSON.stringify({{ok: true}});
}})()"#
    )
}

// JS: locate the composer's model picker WITHOUT relying on its text.
//
// Its label tracks the current model and has read "Instant", "5.6 SolLight" and
// "6Pro" on one account inside three weeks, so any word list goes stale. What is
// stable is where it sits: the composer toolbar row, identified by the plus
// button's testid, holding exactly one other `aria-haspopup="menu"` button.
// JS: find the composer's model / effort picker button. ChatGPT names it in
// several ways across rollouts (selectors as recorded by
// miuuyy/codex-chatgpt-web `chatgpt-session.ts`, Oct 2026); the first group
// with exactly ONE visible match wins. The old structural rule — the one menu
// button on the composer row beside `composer-plus-btn` — is the fallback.
// Never matched on its label, which changes with the model line-up.
const JS_FIND_PICKER: &str = r#"(() => {
  const visible = (b) => { const r = b.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
  const at = (b, via) => {
    const r = b.getBoundingClientRect();
    return JSON.stringify({ok: true, via, label: (b.textContent || '').trim(),
                           x: Math.round(r.left + r.width / 2), y: Math.round(r.top + r.height / 2)});
  };
  for (const [via, sel] of [
    ['testid', 'button[data-testid="model-switcher-dropdown-button"][aria-haspopup="menu"]'],
    ['trigger', 'button[data-codex-intelligence-trigger="true"][aria-haspopup="menu"]'],
    ['tone', 'button[aria-haspopup="menu"][data-tone="neutral"]'],
  ]) {
    const hits = [...document.querySelectorAll(sel)].filter(visible);
    if (hits.length === 1) return at(hits[0], via);
  }
  const plus = document.querySelector('[data-testid="composer-plus-btn"]');
  if (!plus) return JSON.stringify({ok: false, error: 'composer toolbar not found'});
  const rowTop = plus.getBoundingClientRect().top;
  const hits = [...document.querySelectorAll('button[aria-haspopup="menu"]')].filter(b => {
    if (b.getAttribute('data-testid') === 'composer-plus-btn') return false;
    const r = b.getBoundingClientRect();
    return r.width > 0 && r.height > 0 && Math.abs(r.top - rowTop) < 6;
  });
  if (hits.length !== 1) {
    return JSON.stringify({ok: false,
      error: 'expected one model picker on the composer row, found ' + hits.length});
  }
  return at(hits[0], 'row');
})()"#;

// JS: read the opened picker — the effort slider (index + thumb position) and
// the model-family radios. `level` is the name the page currently shows for the
// slider position; it is for logging only, never for matching. The picker's
// content need not be a [role="menu"] any more: it can be a plain container
// with data-testid="composer-intelligence-picker-content", or a [role="group"].
const JS_PICKER_MENU: &str = r#"(() => {
  const SLIDER = '[data-model-reasoning-effort-slider] [role="slider"], [data-model-picker-power-slider] [role="slider"]';
  let menu = null;
  for (const sel of [
    '[data-testid="composer-intelligence-picker-content"]',
    '[role="menu"]:has([role="menuitemradio"], [data-model-reasoning-effort-slider], [data-model-picker-power-slider])',
    '[role="group"]:has([role="menuitemradio"], [data-model-reasoning-effort-slider], [data-model-picker-power-slider])',
    '[role="menu"]',
  ]) {
    menu = document.querySelector(sel);
    if (menu) break;
  }
  const sl = (menu && (menu.querySelector(SLIDER) || menu.querySelector('[role="slider"]')))
    || document.querySelector(SLIDER) || document.querySelector('[role="slider"]');
  let slider = null;
  if (sl) {
    const r = sl.getBoundingClientRect();
    slider = {now: Number(sl.getAttribute('aria-valuenow')),
              min: Number(sl.getAttribute('aria-valuemin')),
              max: Number(sl.getAttribute('aria-valuemax')),
              thumbX: Math.round(r.left + r.width / 2),
              thumbY: Math.round(r.top + r.height / 2)};
  }
  const shown = [...document.querySelectorAll('[role="menuitem"]')]
    .find(e => e.getAttribute('aria-label') === 'Select model');
  const radios = [...document.querySelectorAll('[role="menuitemradio"]')].map(e => ({
    text: (e.textContent || '').trim(),
    checked: e.getAttribute('aria-checked') === 'true'
  }));
  return JSON.stringify({open: !!menu, slider, radios,
                         level: shown ? (shown.textContent || '').trim() : ''});
})()"#;

/// JS: click the model-family radio whose text matches exactly. Unlike the
/// picker button and the slider thumb, these DO respond to `element.click()`.
fn js_click_radio(text: &str) -> String {
    let t = serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_string());
    format!(
        r#"(() => {{
  const target = {t};
  for (const el of document.querySelectorAll('[role="menuitemradio"]')) {{
    if ((el.textContent || '').trim() === target) {{
      el.click();
      return JSON.stringify({{ok: true}});
    }}
  }}
  return JSON.stringify({{ok: false}});
}})()"#
    )
}

// JS: resolve or create a ChatGPT Project by exact display name.
// Returns {ok, id, created, error?}. Mirrors _JS_ENSURE_PROJECT in chatgpt-imagegen.
fn js_ensure_project(name: &str) -> String {
    let name_json = serde_json::to_string(name).unwrap_or_else(|_| "\"\"".to_string());
    format!(
        r#"(async () => {{
  {prelude}
  try {{
    const name = {name_json};
    const tok = await __cguToken();
    if (!tok) return JSON.stringify({{ok: false, error: 'no accessToken in /api/auth/session'}});
    const h = {{Authorization: 'Bearer ' + tok, 'Content-Type': 'application/json'}};
    const find = async () => {{
      const r = await fetch(
        '/backend-api/gizmos/snorlax/sidebar?conversations_per_gizmo=0',
        {{credentials: 'include', headers: h}});
      if (!r.ok) throw new Error('project list HTTP ' + r.status);
      for (const it of (await r.json()).items || []) {{
        const g = it.gizmo && it.gizmo.gizmo;
        if (g && g.display && g.display.name === name) return g.id;
      }}
      return null;
    }};
    let id = await find();
    if (id) return JSON.stringify({{ok: true, id, created: false}});
    const mk = await fetch('/backend-api/projects', {{
      method: 'POST', credentials: 'include', headers: h,
      body: JSON.stringify({{name: name, instructions: ''}})}});
    if (!mk.ok) return JSON.stringify({{ok: false, error: 'project create HTTP ' + mk.status}});
    const j = await mk.json().catch(() => null);
    id = j && ((j.gizmo && (j.gizmo.id || (j.gizmo.gizmo && j.gizmo.gizmo.id))) || j.id);
    if (!id) id = await find();
    if (!id) return JSON.stringify({{ok: false, error: 'created but could not resolve id'}});
    return JSON.stringify({{ok: true, id, created: true}});
  }} catch (e) {{ return JSON.stringify({{ok: false, error: String(e)}}); }}
}})()"#,
        prelude = JS_TOKEN_PRELUDE,
        name_json = name_json
    )
}

#[derive(Debug, Clone)]
pub struct ChannelOptions {
    /// auto | relay | a Chrome profile name.
    pub profile: String,
    /// chrome-use session name (None → derive a per-pid default).
    pub session: Option<String>,
    /// ChatGPT Project name to file the conversation under ("" → plain chat).
    pub project: String,
    /// Per-turn wall-clock budget in seconds.
    pub timeout_secs: u64,
    /// Browser-channel model to select: pro | thinking | instant | <raw label>.
    /// None → use the account default. (Pro is reachable only via the browser.)
    pub model: Option<String>,
    /// Fail with `ErrorKind::Busy` instead of queueing behind another run.
    pub busy_fail: bool,
    /// Receipt to update as the turn progresses (`ask --request-id`).
    pub receipt: Option<PathBuf>,
    /// Connect even during an account cooldown (see `throttle`).
    pub ignore_cooldown: bool,
}

/// Tuning for how `send` decides a reply is COMPLETE. Multi-step connector turns
/// (read → bash build → report) stream in phases with quiet gaps; these knobs let
/// long-running modes (`work`) wait through a several-minute build that a one-shot
/// `ask` would never need.
#[derive(Debug, Clone, Copy)]
pub struct SendOptions {
    /// Polls (each ~2s) the visible reply text must stay UNCHANGED, with no
    /// stop-button and no active-tool marker, before the record is asked. It
    /// never closes a turn by itself; it only stops us hammering the record
    /// while the page is still streaming.
    pub stable_needed: u32,
    /// The page has produced a new assistant turn but its text never goes
    /// stable (collapsible tool disclosures keep mutating it). After this many
    /// consecutive polls the record is asked anyway. The per-turn
    /// `timeout_secs` deadline is still the only thing that ends the wait.
    pub idle_limit: u32,
}

impl Default for SendOptions {
    fn default() -> Self {
        // One-shot defaults: ~4s of unchanged text confirms; ~60s of silence aborts.
        SendOptions { stable_needed: 2, idle_limit: 30 }
    }
}

impl SendOptions {
    /// Generous settings for closed-loop `work`: a build/test can sit quiet for
    /// minutes, so confirm finality more slowly (~6s) and tolerate ~3min of
    /// silence before the safety net fires (the wall-clock `timeout_secs` is the
    /// real ceiling).
    pub fn work() -> Self {
        SendOptions { stable_needed: 3, idle_limit: 90 }
    }
}

/// A live conversation. `send` keeps appending turns to the SAME chat, so
/// ChatGPT retains context across calls — the multi-turn loops (run/serve) only
/// send the new turn, not the whole history.
pub struct Channel {
    /// Resolved path to the chrome-use binary.
    ab: PathBuf,
    /// chrome-use session name.
    session: String,
    /// Per-turn timeout in seconds.
    timeout_secs: u64,
    /// Project to file the conversation under ("" → plain chat). Kept so a
    /// reconnect can rebuild the same starting point.
    project: String,
    /// The conversation this channel is pinned to, latched after the first
    /// successful turn (a fresh chat has no id until then). Every later turn
    /// verifies the tab still shows it, so a sidebar click, a stray navigation
    /// or a project opening a new chat can't silently redirect us into a
    /// DIFFERENT conversation — which would break the "same chat accumulates
    /// context" contract while still returning a plausible-looking reply.
    convo_id: Option<String>,
    /// A project this channel could not ENTER but should still file into, set
    /// when the project page fails to render. Cleared once the conversation has
    /// been moved server-side.
    pending_project: Option<String>,
    /// Whether the current turn's prompt is known to be on the server; decides
    /// how a failure of that turn is typed (see `classify`).
    submitted: bool,
    /// The prompt the current turn submitted, so a record read can prove the
    /// latest user turn in it is OURS and not an earlier question. `None` when
    /// the channel attached to a conversation without sending (see `attach`).
    sent_prompt: Option<String>,
    /// Receipt kept current as the turn progresses (see `receipt`).
    receipt: Option<PathBuf>,
    /// A ChatGPT app (connector) to @-mention into every message, so its tools
    /// are available in that turn (see `attach_connector`).
    connector: Option<String>,
    /// Exclusive claim on the shared ChatGPT window, released when the channel
    /// is dropped or closed.
    _surface: SurfaceLock,
}

impl Channel {
    /// Connect: find chrome-use, choose a logged-in browser, open ChatGPT (in
    /// the project if set), and wait for the composer. Errors clearly if no
    /// logged-in browser is available or the account is rate-limited.
    pub fn connect(opts: &ChannelOptions) -> Result<Self> {
        // An account still cooling down from a recent throttle gets no new
        // page load: that is exactly the traffic that keeps it throttled.
        if !opts.ignore_cooldown {
            check_cooldown()?;
        }

        // Take the surface BEFORE touching the browser: opening the tab and
        // entering a project already mutate the shared window.
        let surface = SurfaceLock::acquire(opts.busy_fail)?;

        let ab = find_chrome_use().ok_or_else(|| {
            anyhow!(
                "`chrome-use` is not installed — install it (no npm, no token):\n  \
                 curl -fsSL https://raw.githubusercontent.com/leeguooooo/chrome-use/main/install.sh | sh"
            )
        })?;

        let session = opts
            .session
            .clone()
            .unwrap_or_else(|| DEFAULT_SESSION.to_string());

        let timeout_secs = opts.timeout_secs;

        // Build candidate profile list (mirrors chatgpt-imagegen run_web logic).
        // None in the list means "relay" (no --profile flag to chrome-use).
        let profile_lower = opts.profile.trim().to_lowercase();
        let candidates: Vec<Option<String>> = match profile_lower.as_str() {
            "relay" | "off" | "current" => vec![None],
            "auto" => {
                // relay first, then any offline-detected logged-in profiles.
                let mut v: Vec<Option<String>> = vec![None];
                v.extend(detect_logged_in_profiles().into_iter().map(Some));
                v
            }
            _ => vec![Some(opts.profile.trim().to_string())],
        };

        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        let mut opened = false;
        // "Sign in" is advice only for a page that actually asked for it. A
        // relay that cannot attach, or a page that never renders, used to end
        // in the same "no logged-in browser" line — sending the user off to
        // fix the one thing that was working.
        let mut saw_login = false;
        let mut last_failure: Option<String> = None;

        // Reuse the tab this session already has, if it is a usable ChatGPT
        // page. Navigating instead costs 45 backend-api requests to re-boot the
        // SPA; the app's own "New chat" costs 1. The throttle that has been
        // biting this account counts REQUESTS, not messages, so this is the
        // difference between one run and forty-five as far as it is concerned.
        {
            let probe = ab_eval(&ab, JS_NEW_CHAT_IN_PLACE, &session, 15.0);
            let reused = probe
                .as_ref()
                .ok()
                .and_then(|v| v.get("ok"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if reused {
                let settle = Instant::now() + Duration::from_secs(20);
                if wait_composer(&ab, &session, settle, 20).unwrap_or(false) {
                    eprintln!("reusing the open ChatGPT tab (new chat, no page reload)");
                    opened = true;
                }
            }
        }

        for prof in &candidates {
            if opened {
                break;
            }
            let label = prof.as_deref().unwrap_or("current Chrome (relay)");
            eprintln!("opening ChatGPT via {label}");

            // Observed twice: opening right after the previous run closed its tab
            // gave a page whose composer never appeared, and an unchanged rerun
            // seconds later worked. So a candidate whose page merely failed to
            // render gets one more try before we move on.
            let mut opened_now = try_open(&ab, &session, WEB_NEW_CHAT_URL, prof.as_deref(), deadline);
            if matches!(opened_now, Ok(false)) {
                let (login, what) = page_probe(&ab, &session);
                if !login {
                    eprintln!("the ChatGPT page never showed its composer ({what}); retrying once");
                    ab_close(&ab, &session);
                    std::thread::sleep(Duration::from_secs(3));
                    opened_now = try_open(&ab, &session, WEB_NEW_CHAT_URL, prof.as_deref(), deadline);
                }
            }
            if matches!(&opened_now, Err(e) if tab_gone(&format!("{e:#}"))) {
                // The session is bound to a tab chrome-use has lost (seen after
                // chrome-use updated itself mid-session). Closing the session
                // drops the dead binding; the next open gets a fresh tab.
                eprintln!("the chrome-use session lost its tab; closing it and opening a fresh one");
                ab_close(&ab, &session);
                opened_now = try_open(&ab, &session, WEB_NEW_CHAT_URL, prof.as_deref(), deadline);
            }
            match opened_now {
                Ok(true) => {
                    eprintln!("using {label}");
                    opened = true;
                    break;
                }
                Ok(false) => {
                    // composer never appeared — try next candidate
                    let (login, what) = page_probe(&ab, &session);
                    if login {
                        saw_login = true;
                    } else {
                        last_failure = Some(format!(
                            "{label}: the ChatGPT page loaded ({what}) but showed no composer. \
                             Most often this Chrome profile is signed out of ChatGPT (the signed-out \
                             page has no composer): open chatgpt.com in it and sign in. If it is \
                             signed in, ChatGPT may have changed its editor and chatgpt-use needs \
                             an update"
                        ));
                    }
                    ab_close(&ab, &session);
                }
                Err(e) => {
                    ab_close(&ab, &session);
                    let msg = e.to_string();
                    if msg.contains("rate-limited") || msg.contains("Too many") {
                        return Err(rate_limited(RATE_LIMIT_MSG));
                    }
                    // A chrome-use session name can go temporarily unusable: a
                    // command that runs too long is judged unresponsive and its
                    // daemon is stopped, after which every command on that NAME
                    // returns the same error for a while. It does clear on its
                    // own — observed recovering roughly an hour later — but none
                    // of `session stop --force`, deleting the lifecycle lock or
                    // upgrading the CLI made it clear on demand.
                    //
                    // Trying the next candidate cannot help, and falling through
                    // to "no logged-in ChatGPT browser available. Sign in to
                    // chatgpt.com" tells the user to fix the one thing that is
                    // not broken. Say what happened and give a way through now.
                    if msg.contains("session unresponsive") || msg.contains("stuck") {
                        return Err(ChannelError::new(ErrorKind::SessionUnavailable, format!(
                            "the chrome-use session {session:?} is wedged — every command on \
                             that name is returning \"session unresponsive\". You are still \
                             signed in; this is not a login problem.\n\n  Use another name \
                             meanwhile:  chatgpt-use <cmd> --session chatgpt-web-2\n\n\
                             It is usually caused by one very long chrome-use command (a large \
                             `keyboard inserttext`, say) being judged unresponsive. The name \
                             frees itself later — about an hour, in the case we measured — so \
                             the original is worth retrying rather than abandoning."
                        ))
                        .into());
                    }
                    // other errors: log and try the next candidate
                    eprintln!("warning: {label} failed: {e}");
                    last_failure = Some(format!("{label}: {e}"));
                }
            }
        }

        if let (false, false, Some(last)) = (opened, saw_login, &last_failure) {
            return Err(ChannelError::new(
                ErrorKind::SessionUnavailable,
                format!(
                    "could not open ChatGPT through chrome-use (tried {} candidate(s)). Last \
                     failure — {last}\n\n  Check:  chrome-use status",
                    candidates.len()
                ),
            )
            .into());
        }
        if !opened {
            return Err(ChannelError::new(
                ErrorKind::LoginRequired,
                format!(
                    "no logged-in ChatGPT browser available (tried {} candidate(s)). \
                     Sign in to chatgpt.com in Chrome.",
                    candidates.len()
                ),
            )
            .into());
        }

        let mut chan = Channel {
            ab,
            session,
            timeout_secs,
            project: opts.project.trim().to_string(),
            convo_id: None,
            pending_project: None,
            submitted: false,
            sent_prompt: None,
            receipt: opts.receipt.clone(),
            connector: None,
            _surface: surface,
        };

        // Navigate into a ChatGPT Project FIRST — it loads a new page and would
        // reset any model selection, so model selection must come afterwards.
        let project = opts.project.trim().to_string();
        if !project.is_empty() {
            let proj_deadline = Instant::now() + Duration::from_secs(timeout_secs);
            match chan.resolve_project(&project, proj_deadline) {
                Ok((gizmo, created)) => {
                    eprintln!(
                        "using project {project:?}{}",
                        if created { " (created)" } else { "" }
                    );
                    if let Err(e) = chan.open_project_page(&gizmo, proj_deadline) {
                        // The project page would not render. That is ChatGPT's
                        // front end failing, not a reason for --project to stop
                        // working: the backend still files conversations fine,
                        // so start in a plain chat and move it afterwards.
                        eprintln!(
                            "warning: the project page didn't load ({e}); starting in a plain \
                             chat and filing the conversation into {project:?} afterwards"
                        );
                        chan.pending_project = Some(gizmo);
                        let restore_deadline = Instant::now() + Duration::from_secs(30);
                        let _ =
                            ab_open(&chan.ab, &chan.session, WEB_NEW_CHAT_URL, None, restore_deadline);
                        let _ = wait_composer(&chan.ab, &chan.session, restore_deadline, 15);
                    }
                }
                Err(e) => {
                    // Could not even resolve the project (no id), so there is
                    // nothing to file into later.
                    eprintln!("warning: project {project:?} unavailable ({e}); using a plain chat");
                }
            }
        }

        // Apply an explicitly requested model on the now-settled composer (after
        // any project navigation).
        //
        // This used to warn and carry on with the account default. That quietly
        // misrepresents the run: `--model pro` reports success while answering
        // from some other model, and `work` — which must stay OFF Pro, because
        // Pro cannot use Apps/MCP — silently loses every connector tool and then
        // looks like a model that "won't use its tools". Only ever set when the
        // caller named a model, so erring here refuses exactly the request we
        // cannot honour.
        if let Some(ref model) = opts.model.as_ref().filter(|m| !keeps_current_model(m)) {
            let model_deadline = Instant::now() + Duration::from_secs(timeout_secs.min(30));
            chan.select_model(model, model_deadline).with_context(|| {
                format!(
                    "could not select model {model:?} — refusing to run on the account \
                     default instead. ChatGPT relabelled the composer picker from \
                     Intelligence levels (instant/high/pro) to model names \
                     (e.g. \"5.6 SolLight\"), so the selector needs updating; rerun \
                     with --model current to accept whatever the account is set to"
                )
            })?;
        }

        Ok(chan)
    }

    /// Put `message` in the composer and submit it, returning only once a new
    /// user turn proves the submit landed.
    fn fill_and_submit(
        &self,
        message: &str,
        attachments: &[PathBuf],
        baseline_users: &UserTurns,
        budget: f64,
    ) -> std::result::Result<(), SubmitFailure> {
        self.fill_composer(message, attachments, budget)
            .map_err(SubmitFailure::BeforeSubmit)?;
        self.submit(baseline_users, budget)
    }

    /// Put `message` in the composer and verify it landed intact. Nothing here
    /// can have submitted anything, so any error is safe to retry.
    /// @-mention the ChatGPT app `name` into every message from now on, so the
    /// turn can call its tools. A custom MCP app is not available in a chat
    /// by default; mentioning it is how a message opts in.
    pub fn use_connector(&mut self, name: &str) {
        self.connector = Some(name.to_string()).filter(|n| !n.trim().is_empty());
    }

    /// Put the app `name` into the (empty, focused) composer as a mention
    /// pill: type `@name` with real keystrokes, require exactly one matching
    /// row in the mention menu, highlight it with the arrow keys, press Enter,
    /// then confirm the pill. Fails closed: a message that needed the app's
    /// tools is not sent without them.
    fn attach_connector(&self, name: &str, budget: f64) -> Result<()> {
        let menu = |ch: &Self| {
            ab_eval(&ch.ab, &js_mention_menu(name), &ch.session, budget)
                .ok()
                .filter(|v| v.is_object())
                .unwrap_or_default()
        };
        let count = |v: &serde_json::Value| v.get("count").and_then(|c| c.as_u64()).unwrap_or(0);
        let mut seen = serde_json::Value::Null;
        for attempt in 0..3 {
            if attempt > 0 {
                let _ = ab_eval(&self.ab, JS_CLEAR_COMPOSER, &self.session, budget);
            }
            ab_cmd(&self.ab, &["click", "[data-cgu-composer]"], &self.session, budget)
                .context("focusing the composer for the app mention")?;
            ab_cmd(&self.ab, &["keyboard", "type", &format!("@{name}")], &self.session, budget)
                .context("typing the app mention")?;
            let until = Instant::now() + Duration::from_millis(2500);
            loop {
                seen = menu(self);
                if count(&seen) == 1 || Instant::now() >= until {
                    break;
                }
                std::thread::sleep(Duration::from_millis(250));
            }
            if count(&seen) == 1 {
                break;
            }
        }
        if count(&seen) != 1 {
            let _ = ab_eval(&self.ab, JS_CLEAR_COMPOSER, &self.session, budget);
            let titles = seen.get("titles").cloned().unwrap_or_default();
            bail!(
                "the ChatGPT app {name:?} is not available to this chat (mention menu showed \
                 {titles}). In ChatGPT, check Settings → Apps that {name:?} is connected and \
                 enabled, and that its server and tunnel are up; then `chatgpt-use refresh`."
            );
        }
        let rows = seen.get("titles").and_then(|t| t.as_array()).map_or(1, |a| a.len());
        for _ in 0..rows {
            if seen.get("highlighted").and_then(|h| h.as_bool()).unwrap_or(false) {
                break;
            }
            let _ = ab_cmd(&self.ab, &["press", "ArrowDown"], &self.session, budget);
            seen = menu(self);
        }
        if !seen.get("highlighted").and_then(|h| h.as_bool()).unwrap_or(false) {
            let _ = ab_eval(&self.ab, JS_CLEAR_COMPOSER, &self.session, budget);
            bail!("could not highlight the {name:?} row in the mention menu");
        }
        ab_cmd(&self.ab, &["press", "Enter"], &self.session, budget)
            .context("choosing the app in the mention menu")?;
        std::thread::sleep(Duration::from_millis(300));
        let pill = ab_eval(&self.ab, &js_connector_pill(name), &self.session, budget).unwrap_or_default();
        if !pill.get("ok").and_then(|b| b.as_bool()).unwrap_or(false) {
            let _ = ab_eval(&self.ab, JS_CLEAR_COMPOSER, &self.session, budget);
            bail!("chose {name:?} in the mention menu but no pill for it appeared ({pill})");
        }
        eprintln!("attached the ChatGPT app {name:?} to this message");
        Ok(())
    }

    fn fill_composer(&self, message: &str, attachments: &[PathBuf], budget: f64) -> Result<()> {
        // Never type while the PAGE still believes it is generating. ChatGPT
        // disables submission then, so Enter is silently swallowed and the turn
        // reports "never submitted".
        //
        // This became reachable when replies started coming from the server: the
        // record says `end_turn` the moment the turn closes, which can be while
        // the page is still rendering the tail, so we can now return from one
        // turn and start the next before the composer is willing. Bounded, and
        // it gives up rather than blocking — a stop button that never clears is
        // its own problem and the submit check below will report it honestly.
        let busy = |ch: &Self| {
            ab_eval(&ch.ab, JS_STATE, &ch.session, budget)
                .ok()
                .filter(|v| v.is_object())
                .map(|v| {
                    v.get("stop").and_then(|b| b.as_bool()).unwrap_or(false)
                        || v.get("tool_active").and_then(|b| b.as_bool()).unwrap_or(false)
                })
                .unwrap_or(false)
        };
        let wait_idle = |ch: &Self, secs: u64| {
            let until = Instant::now() + Duration::from_secs(secs);
            while busy(ch) && Instant::now() < until {
                std::thread::sleep(Duration::from_millis(500));
            }
        };

        wait_idle(self, 10);
        if busy(self) {
            // The page is stuck mid-generation and will not recover on its own.
            // Seen live on an account whose front end had degraded: after a turn
            // the stop button stayed present indefinitely — still there 32s
            // later — so the composer refused every further message while the
            // server had long since closed the turn. Waiting longer does not
            // help; reloading the conversation does. This is the same trade the
            // rest of the channel makes: the record is authoritative, the page
            // is just a keyboard, and a keyboard that has locked up gets reset.
            eprintln!("the page is stuck mid-generation; reloading the conversation to free the composer");
            if self.convo_id.is_some() {
                self.reopen_pinned(budget)
                    .context("reloading a page stuck mid-generation")?;
            }
            wait_idle(self, 10);
        }

        // Focus and empty the composer, then insert the message as TEXT. The
        // probe stamps the composer it finds (`js_composer_helper!`), and the
        // real-input click targets that stamp.
        let found = ab_eval(&self.ab, JS_COMPOSER, &self.session, budget)
            .ok()
            .and_then(|v| v.get("composer").and_then(|b| b.as_bool()))
            .unwrap_or(false);
        if !found {
            bail!("no single visible ChatGPT composer to type into");
        }
        ab_cmd(&self.ab, &["click", "[data-cgu-composer]"], &self.session, budget)
            .context("clicking the composer")?;
        // Clear, then CONFIRM the composer is actually empty. One `delete` is not
        // enough after a reattach: the page may still be hydrating, and ChatGPT
        // restores a saved draft into the composer once it is — which silently
        // prepends a stray character to the prompt. (Seen live: a 26 KB payload
        // arrived one character long, which the integrity check below correctly
        // rejected.) Whitespace is ignored, so only real leftover text blocks us.
        let mut cleared = false;
        for _ in 0..5 {
            let _ = ab_eval(&self.ab, JS_CLEAR_COMPOSER, &self.session, budget);
            if ab_eval(&self.ab, JS_COMPOSER_FINGERPRINT, &self.session, budget)
                .ok()
                .and_then(|v| v.get("n").and_then(|n| n.as_u64()))
                == Some(0)
            {
                cleared = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(300));
        }
        if !cleared {
            bail!(
                "could not empty the ChatGPT composer — leftover text would be \
                 prepended to the message"
            );
        }
        if let Some(name) = self.connector.clone() {
            self.attach_connector(&name, budget)?;
        }

        // Insert in chunks — a multi-KB argument overruns chrome-use's IPC and
        // fails with EAGAIN ("Resource temporarily unavailable"). Each chunk
        // appends at the caret. Split on char boundaries (prompts contain
        // multibyte text). See `js_insert_text` for why this is not `keyboard
        // type`: typed newlines submit, which silently shredded every multi-line
        // prompt into one chat message per line.
        const INSERT_CHUNK_CHARS: usize = 1500;
        let chars: Vec<char> = message.chars().collect();
        for chunk in chars.chunks(INSERT_CHUNK_CHARS) {
            let piece: String = chunk.iter().collect();
            let res = ab_eval(&self.ab, &js_insert_text(&piece), &self.session, budget)
                .context("inserting message text into composer")?;
            if !res.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                bail!("could not insert text into the ChatGPT composer");
            }
        }

        // Integrity check BEFORE submitting: confirm the whole payload is sitting
        // in the composer, in the right ORDER. Cheaper and far more useful than
        // discovering a mangled prompt from a confused reply ten minutes later.
        //
        // Both halves matter. The count catches truncation (a chunk that never
        // landed) and pollution (a restored draft prepended); the hash catches
        // reordering, which the count cannot see because it preserves length —
        // and reordering is not hypothetical here, it is what
        // leeguooooo/chrome-use#301 does to chunked inserts.
        let (want_n, want_h) = composer_fingerprint(message);
        let got = ab_eval(&self.ab, JS_COMPOSER_FINGERPRINT, &self.session, budget).ok();
        check_fingerprint(got.as_ref(), want_n, want_h as u64)?;
        // Attachments go in AFTER the fingerprint: a chip rendered inside the
        // composer would otherwise change the hash this check just approved.
        // The click puts focus back, so `submit`'s Enter still lands in the box.
        if !attachments.is_empty() {
            self.upload_attachments(attachments, budget)?;
            ab_cmd(&self.ab, &["click", "[data-cgu-composer]"], &self.session, budget)
                .context("refocusing the composer after attaching files")?;
        }
        Ok(())
    }

    /// Put `paths` in the composer's attachment slot and wait until the page
    /// shows them. Fails closed: a prompt that was supposed to carry files is
    /// not submitted without them.
    fn upload_attachments(&self, paths: &[PathBuf], budget: f64) -> Result<()> {
        let probe = ab_eval(&self.ab, JS_FILE_INPUT, &self.session, budget)
            .context("looking for the composer's file input")?;
        if !probe.get("ok").and_then(|b| b.as_bool()).unwrap_or(false) {
            bail!(
                "the ChatGPT composer exposes no file input, so the attachment(s) cannot be \
                 added: {}",
                names(paths)
            );
        }
        let existing = probe.get("existing").and_then(|n| n.as_u64()).unwrap_or(0);
        if existing > 0 {
            bail!(
                "the ChatGPT composer already holds {existing} attachment(s), which would ride \
                 along with this message; reload the chat and try again"
            );
        }

        let args = upload_args(paths)?;
        let argv: Vec<&str> = args.iter().map(String::as_str).collect();
        ab_cmd(&self.ab, &argv, &self.session, budget)
            .context("uploading attachments through chrome-use")?;

        // chrome-use confirms the input took the files, but the app may move
        // them into its own chip list immediately. Ask the page, not the exit
        // code, and give the upload a generous window before refusing.
        let want = existing + paths.len() as u64;
        let until = Instant::now() + Duration::from_secs(15);
        loop {
            let chips = ab_eval(&self.ab, JS_ATTACHMENT_CHIPS, &self.session, budget)
                .ok()
                .and_then(|v| v.get("chips").and_then(|n| n.as_u64()))
                .unwrap_or(0);
            if chips >= want {
                return Ok(());
            }
            if Instant::now() >= until {
                bail!(
                    "uploaded {} but the composer never showed them; refusing to send the \
                     prompt without its attachments",
                    names(paths)
                );
            }
            std::thread::sleep(Duration::from_millis(300));
        }
    }

    /// Press Enter and return only once a new user turn proves it landed.
    fn submit(&self, baseline_users: &UserTurns, budget: f64) -> std::result::Result<(), SubmitFailure> {
        // From here on a retry could DUPLICATE the message, so every failure is
        // reported as ambiguous and the caller must not resend blindly.
        ab_cmd(&self.ab, &["press", "Enter"], &self.session, budget)
            .context("pressing Enter to submit")
            .map_err(SubmitFailure::Ambiguous)?;

        // Confirm the submit actually landed, by EVIDENCE (a new user turn was
        // rendered) rather than by the old proxy "is the composer empty?". That
        // proxy raced React's clear and misread in both directions: a slow clear
        // looked like a failed submit (→ duplicate send), and a swallowed Enter
        // with an already-cleared box looked like success (→ we then waited on,
        // and scraped, the PREVIOUS turn).
        if !self.await_user_turn(baseline_users, Duration::from_secs(3), budget) {
            // Enter didn't take. Click the send button and demand evidence again.
            // Note this fallback is naturally inert if the submit did land after
            // all: once generation starts, the send button becomes the stop
            // button (`stop-button` / `composer-stop-button`, "Stop streaming")
            // and none of SEND_BUTTON_SELECTOR's alternatives match it.
            let _ = ab_cmd(
                &self.ab,
                &["click", SEND_BUTTON_SELECTOR],
                &self.session,
                budget,
            );
            if !self.await_user_turn(baseline_users, Duration::from_secs(5), budget) {
                // Last resort: a synthetic Enter on the composer itself. Untrusted
                // events are weaker than the CDP `press Enter` above, but the
                // editor's own keydown handler may still act on one. Harmless if
                // the submit did land late: the composer is empty by then.
                let _ = ab_eval(
                    &self.ab,
                    JS_DISPATCH_ENTER,
                    &self.session,
                    budget,
                );
                if !self.await_user_turn(baseline_users, Duration::from_secs(5), budget) {
                    return Err(SubmitFailure::Ambiguous(anyhow!(
                        "the message was never submitted: no new user turn appeared \
                         after pressing Enter, clicking the send button, and dispatching \
                         fallback Enter. The composer may be disabled (rate limit, expired \
                         session) or the page layout changed."
                    )));
                }
            }
        }

        Ok(())
    }

    /// The conversation record for the pinned conversation, parsed. `None`
    /// when there is nothing to ask about or the read failed — never an error,
    /// because a missing record is "no proof", not a failed turn.
    fn record(&self, budget: f64) -> Option<RecordTurn> {
        let id = self.convo_id.as_ref()?;
        let res = ab_eval(&self.ab, &js_server_final(id), &self.session, budget).ok()?;
        if !res.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
            return None;
        }
        RecordTurn::from_json(&res)
    }

    /// What the record proves about this turn. The page is a hint; this is the
    /// only source allowed to close a turn.
    fn record_verdict(&self, budget: f64) -> Verdict {
        let Some(turn) = self.record(budget) else {
            return Verdict::Unreadable;
        };
        if !record_is_ours(
            &turn,
            self.convo_id.as_deref(),
            self.sent_prompt.as_deref(),
            self.connector.is_some(),
        ) {
            return Verdict::Unreadable;
        }
        judge_record(&turn)
    }

    /// Move an existing conversation into a Project through the backend API.
    fn file_into_project(&self, convo_id: &str, gizmo_id: &str, budget: f64) -> Result<()> {
        let res = ab_eval(
            &self.ab,
            &js_file_into_project(convo_id, gizmo_id),
            &self.session,
            budget,
        )
        .context("calling the project-filing endpoint")?;
        if res.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
            return Ok(());
        }
        let detail = res.get("error").and_then(|v| v.as_str()).unwrap_or("unknown");

        // This is the one place a remembered gizmo id gets tested against the
        // server, so it is where a stale one has to be dropped. A project that
        // was deleted, or that belongs to a different account than the browser
        // is now signed into, would otherwise be retried from cache forever —
        // the cache would have turned a transient mistake into a permanent one.
        if detail.contains("404") || detail.contains("400") || detail.contains("403") {
            forget_gizmo(&self.project);
            eprintln!(
                "forgetting the remembered id for project {:?} ({detail}); it will be \
                 looked up again next run",
                self.project
            );
        }
        bail!("{detail}")
    }

    /// The conversation UUID currently shown in the tab, if any.
    fn current_convo_id(&self, budget: f64) -> Option<String> {
        ab_eval(&self.ab, JS_CONVO_ID, &self.session, budget)
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .filter(|s| !s.is_empty())
    }

    /// Fail closed if the tab has drifted off the conversation we pinned.
    ///
    /// A `None` pin means "not latched yet" (fresh chat, first turn) and always
    /// passes. Once latched, a mismatch is never recoverable by carrying on:
    /// the multi-turn modes rely on context accumulated in the pinned chat, so
    /// answering from a different one is worse than erroring.
    fn verify_convo(&self, budget: f64) -> Result<()> {
        let Some(ref pinned) = self.convo_id else { return Ok(()) };
        if convo_drift(pinned, self.current_convo_id(budget).as_deref()).is_none() {
            return Ok(());
        }
        // Drifted. Try once to steer back before giving up — a stray navigation
        // or a sidebar click is recoverable, and the conversation itself (with
        // all our accumulated context) is still there on the server.
        self.reopen_pinned(budget)?;
        match convo_drift(pinned, self.current_convo_id(budget).as_deref()) {
            None => Ok(()),
            Some(msg) => bail!("{msg}"),
        }
    }

    /// Re-establish a FRESH chat after losing the tab before anything was said.
    ///
    /// Safe only pre-pin and pre-submit: with no conversation id there is no
    /// context to preserve, and with no submit receipt there is no risk of
    /// duplicating a message that is already generating. This is the ordinary
    /// case when another process sharing the session finishes and closes the
    /// browser out from under us.
    fn reopen_fresh(&self, budget: f64) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs_f64(budget.clamp(20.0, 90.0));
        eprintln!("the ChatGPT tab went away before the first turn; opening a fresh chat");
        let in_place = ab_eval(&self.ab, JS_NEW_CHAT_IN_PLACE, &self.session, budget)
            .ok()
            .and_then(|v| v.get("ok").and_then(|b| b.as_bool()))
            .unwrap_or(false);
        if !(in_place && wait_composer(&self.ab, &self.session, deadline, 20).unwrap_or(false)) {
            ab_open(&self.ab, &self.session, WEB_NEW_CHAT_URL, None, deadline)
                .context("reopening ChatGPT")?;
        }
        if !wait_composer(&self.ab, &self.session, deadline, 30)? {
            bail!("reopened ChatGPT but the composer never appeared");
        }
        if !self.project.is_empty() {
            match self.resolve_project(&self.project, deadline) {
                Ok((gizmo, _)) => {
                    if self.open_project_page(&gizmo, deadline).is_err() {
                        // Same degradation as `connect`: keep the project, lose
                        // only the ability to be born inside it.
                        eprintln!(
                            "warning: the project page didn't load; will file the conversation \
                             into {:?} afterwards",
                            self.project
                        );
                    }
                }
                Err(e) => eprintln!(
                    "warning: project {:?} unavailable ({e}); using a plain chat",
                    self.project
                ),
            }
        }
        Ok(())
    }

    /// Navigate the session back to the pinned conversation.
    ///
    /// This is the whole point of pinning an id: when the tab is closed, the
    /// browser restarts, or the page wanders off, the conversation and any
    /// in-flight generation live on SERVER-side — only our observer was lost.
    /// Reopening `/c/<id>` re-attaches to it, so a turn that would previously
    /// have burned down to a bare "timed out" can carry on.
    fn reopen_pinned(&self, budget: f64) -> Result<()> {
        let Some(ref id) = self.convo_id else {
            bail!(
                "lost contact with the ChatGPT tab before this conversation had an \
                 id (the id only exists once the first turn is persisted), so there \
                 is nothing to reconnect to — rerun the command."
            );
        };
        let deadline = Instant::now() + Duration::from_secs_f64(budget.clamp(20.0, 90.0));
        eprintln!("reattaching to conversation {id}");

        // Sidebar link first: 9 backend-api requests against 45 for a real
        // navigation. This path runs often — every page reset on a degraded
        // front end goes through it — so the difference compounds fast.
        let in_place = ab_eval(&self.ab, &js_open_convo_in_place(id), &self.session, budget)
            .ok()
            .and_then(|v| v.get("ok").and_then(|b| b.as_bool()))
            .unwrap_or(false);
        if in_place && wait_composer(&self.ab, &self.session, deadline, 20).unwrap_or(false) {
            return Ok(());
        }

        let url = format!("{WEB_CONVO_URL_TPL}{id}");
        if let Err(first) = ab_open(&self.ab, &self.session, &url, None, deadline) {
            // Seen live: chrome-use refuses to drive a tab it has lost ("the
            // tab this command was driving is gone") rather than retarget.
            // Closing the session drops that dead binding; the next open gets
            // a fresh tab on the same conversation, as `connect` does.
            if !tab_gone(&format!("{first:#}")) {
                return Err(first).context("reopening the pinned conversation");
            }
            eprintln!("the chrome-use session lost its tab; reopening conversation {id} in a fresh one");
            ab_close(&self.ab, &self.session);
            ab_open(&self.ab, &self.session, &url, None, deadline)
                .context("reopening the pinned conversation in a fresh tab")?;
        }
        if !wait_composer(&self.ab, &self.session, deadline, 30)? {
            bail!("reopened conversation {id} but the composer never appeared");
        }
        Ok(())
    }

    /// The rendered user turns, or `None` if the page couldn't be read.
    fn user_turns(&self, budget: f64) -> Option<UserTurns> {
        ab_eval(&self.ab, JS_USER_TURNS, &self.session, budget)
            .ok()
            .and_then(|v| UserTurns::from_json(&v))
    }

    /// Poll up to `within` for the user-turn count to exceed `baseline` — i.e.
    /// for positive evidence that our submit was accepted.
    fn await_user_turn(&self, baseline: &UserTurns, within: Duration, budget: f64) -> bool {
        let until = Instant::now() + within;
        loop {
            if self.user_turns(budget).is_some_and(|now| user_turn_rose(baseline, &now)) {
                return true;
            }
            if Instant::now() >= until {
                return false;
            }
            std::thread::sleep(Duration::from_millis(300));
        }
    }

    /// Send one message and return ChatGPT's completed reply as text/markdown,
    /// using the default (one-shot) completion tuning.
    pub fn send(&mut self, message: &str) -> Result<String> {
        self.send_with(message, &SendOptions::default())
    }

    /// Send one message with files attached to it. The paths are uploaded to
    /// the composer before submit; a failure to place them refuses the turn
    /// rather than sending the prompt alone.
    pub fn send_attached(&mut self, message: &str, attachments: &[PathBuf]) -> Result<String> {
        self.send_with_attachments(message, &SendOptions::default(), attachments)
    }

    /// The conversation this channel is pinned to, once a turn has latched one.
    pub fn conversation_id(&self) -> Option<&str> {
        self.convo_id.as_deref()
    }

    /// Attach to an existing conversation to read its reply, without typing.
    ///
    /// Opens a plain chat (no project entry, no model change) and uses its tab
    /// only as a signed-in origin for the conversation API: the conversation
    /// itself is never navigated to, typed into or filed, so attaching cannot
    /// disturb it or any other one.
    pub fn attach(opts: &ChannelOptions, convo_id: &str) -> Result<Self> {
        let plain = ChannelOptions {
            profile: opts.profile.clone(),
            session: opts.session.clone(),
            project: String::new(),
            timeout_secs: opts.timeout_secs,
            model: None,
            busy_fail: opts.busy_fail,
            receipt: None,
            ignore_cooldown: opts.ignore_cooldown,
        };
        let mut chan = Channel::connect(&plain)?;
        chan.convo_id = Some(convo_id.to_string());
        chan.submitted = true;
        Ok(chan)
    }

    /// Wait for the pinned conversation's reply to finish, reading only the
    /// server record (`end_turn`). Used to resume a request whose owner went
    /// away; it never sends anything.
    pub fn await_record(&mut self) -> Result<String> {
        let Some(id) = self.convo_id.clone() else {
            bail!("no conversation to wait on");
        };
        let deadline = Instant::now() + Duration::from_secs(self.timeout_secs);
        loop {
            match self.record_verdict(30.0) {
                Verdict::Done(answer) => return Ok(answer.text),
                Verdict::CutOff { kind, reason, chars } => return Err(kind.error(reason, chars)),
                Verdict::Open | Verdict::Unreadable => {}
            }
            if Instant::now() >= deadline {
                let why = "the reply has not finished, or the record could not be read";
                return Err(ChannelError::new(
                    ErrorKind::Incomplete,
                    format!("{why} after {}s (conversation {id})", self.timeout_secs),
                )
                .with_submitted(Submitted::Yes)
                .into());
            }
            nap(Duration::from_secs(5));
            if cancel_requested() {
                // Stop WAITING; the generation itself is not ours to stop here.
                return Err(ChannelError::new(
                    ErrorKind::Incomplete,
                    format!("stopped waiting for conversation {id}; the reply may still be generating"),
                )
                .with_submitted(Submitted::Yes)
                .into());
            }
        }
    }

    /// Honour a cancel mid-turn: press stop, and report `Cancelled` only once
    /// the conversation record confirms the reply stopped.
    ///
    /// Stop is pressed only while the tab shows OUR pinned conversation — the
    /// stop button acts on whatever page is open, and a cancel must never stop
    /// somebody else's reply. A reply that finished before the stop took
    /// effect is returned as the reply it is.
    fn stop_generation(&mut self, budget: f64) -> Result<String> {
        eprintln!("cancel requested; stopping this conversation's reply");
        let give_up = Instant::now() + Duration::from_secs(20);
        loop {
            let current = self.current_convo_id(10.0);
            let on_ours = match &self.convo_id {
                Some(pinned) => convo_drift(pinned, current.as_deref()).is_none(),
                // Turn one, before ChatGPT has assigned an id: the session's
                // tab is the fresh chat this channel opened.
                None => true,
            };
            if on_ours {
                let _ = ab_eval(&self.ab, JS_CLICK_STOP, &self.session, 10.0);
            }
            if self.convo_id.is_none() && current.is_some() {
                self.convo_id = current;
                self.touch_receipt();
            }
            match self.record_verdict(10.0) {
                Verdict::CutOff { kind: CutOffKind::Interrupted, chars, .. } => {
                    return Err(ChannelError::new(
                        ErrorKind::Cancelled,
                        format!(
                            "cancelled: the conversation record confirms the reply stopped \
                             ({chars} characters were generated)"
                        ),
                    )
                    .with_submitted(Submitted::Yes)
                    .into());
                }
                Verdict::CutOff { kind, reason, chars } => return Err(kind.error(reason, chars)),
                Verdict::Done(answer) => {
                    eprintln!("the reply finished before the stop took effect");
                    return self.finish_turn(answer.text, budget);
                }
                Verdict::Open | Verdict::Unreadable => {}
            }
            if Instant::now() >= give_up {
                return Err(ChannelError::new(
                    ErrorKind::CancelRequested,
                    "pressed stop, but the conversation record has not confirmed it; the reply \
                     may still be generating",
                )
                .with_submitted(Submitted::Yes)
                .into());
            }
            std::thread::sleep(Duration::from_secs(2));
        }
    }

    /// Cancel the pinned conversation's reply from a channel made by `attach`,
    /// for a request whose owner is gone. A reply that already finished is
    /// returned (nothing to cancel); one still open is reopened so its own
    /// stop button can be pressed.
    pub fn cancel_pinned(&mut self) -> Result<String> {
        // Only a turn the record proves closed (finished or cut off) has
        // nothing left to cancel. An unreadable record means we cannot tell,
        // and reopening to press stop is the safe side of that.
        let open = !matches!(
            self.record_verdict(20.0),
            Verdict::Done(_) | Verdict::CutOff { .. }
        );
        if open {
            self.reopen_pinned(60.0)?;
        }
        self.stop_generation(60.0)
    }

    /// Record in the receipt, if there is one, what is now known: that the
    /// prompt is on the server, and which conversation it is in.
    fn touch_receipt(&self) {
        let Some(path) = &self.receipt else { return };
        let (submitted, convo) = (self.submitted, self.convo_id.clone());
        crate::receipt::update(path, |r| {
            if submitted {
                r.state = "submitted".into();
                r.submitted = "yes".into();
            }
            if convo.is_some() {
                r.conversation_id = convo;
            }
        });
    }

    /// Text of a visible dialog on the page, if one is up.
    fn blocking_dialog(&self, budget: f64) -> Option<String> {
        let v = ab_eval(&self.ab, JS_BLOCKING_DIALOG, &self.session, budget).ok()?;
        let text = v.get("text")?.as_str()?.trim().to_string();
        (!text.is_empty()).then_some(text)
    }

    /// Send one message with explicit completion tuning (see `SendOptions`).
    ///
    /// A failure carries a [`ChannelError`] saying whether the prompt reached
    /// ChatGPT, so a caller knows if retrying could post it twice.
    pub fn send_with(&mut self, message: &str, sopts: &SendOptions) -> Result<String> {
        self.send_with_attachments(message, sopts, &[])
    }

    fn send_with_attachments(
        &mut self,
        message: &str,
        sopts: &SendOptions,
        attachments: &[PathBuf],
    ) -> Result<String> {
        self.submitted = false;
        let result = self.send_turn(message, sopts, attachments);
        result.map_err(|e| classify(e, self.submitted))
    }

    fn send_turn(
        &mut self,
        message: &str,
        sopts: &SendOptions,
        attachments: &[PathBuf],
    ) -> Result<String> {
        // `serve` holds one channel for hours: a throttle hit on one turn must
        // also hold back the next, not only the next process.
        check_cooldown()?;
        let deadline = Instant::now() + Duration::from_secs(self.timeout_secs);

        let remaining_secs = || {
            deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::from_secs(2))
                .as_secs_f64()
                .max(2.0)
        };

        // Refuse to type into a tab that has drifted off our pinned conversation.
        self.verify_convo(remaining_secs())?;

        // Any deferred project filing happens HERE, before this turn types
        // anything, rather than after the previous one finished.
        //
        // Filing re-routes the open conversation to its project URL and the
        // composer is gone for that moment, so doing it mid-turn broke the next
        // send. Doing it at the START of a turn puts the re-route before the
        // typing, where the idle-wait and page-reset below already absorb it —
        // and unlike filing in `close`, it also covers `serve`, which holds one
        // channel for the life of the process and never closes it.
        if let (Some(gizmo), Some(cid)) = (self.pending_project.clone(), self.convo_id.clone()) {
            match self.file_into_project(&cid, &gizmo, remaining_secs()) {
                Ok(()) => {
                    eprintln!("filed conversation into project {:?}", self.project);
                    self.pending_project = None;
                }
                Err(e) => eprintln!(
                    "warning: could not file the conversation into {:?} ({e}); it stays in a \
                     plain chat",
                    self.project
                ),
            }
        }

        // Snapshot rendered user turns: a rise in this count is our submit
        // receipt (see `user_turn_rose`).
        let mut baseline_users = self.user_turns(remaining_secs()).unwrap_or_default();

        // Snapshot the current number of assistant messages so we can detect
        // when a NEW one arrives.
        let baseline_count: u64 = ab_eval(&self.ab, JS_ASSISTANT_COUNT, &self.session, remaining_secs())
            .ok()
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        // Fill + submit, with ONE reattach-and-retry: the tab can vanish between
        // turns (closed, crashed, browser restarted) and the conversation itself
        // is still on the server, so losing the window shouldn't lose the turn.
        if cancel_requested() {
            return Err(ChannelError::new(
                ErrorKind::Cancelled,
                "cancelled before the prompt was sent",
            )
            .into());
        }

        crate::throttle::note_event(
            crate::throttle::EVENT_SEND,
            serde_json::json!({ "conversation_id": self.convo_id, "chars": message.chars().count() }),
        );
        // Identity for every record read this turn: the latest user message in
        // the record must be this one, or its reply is somebody else's.
        self.sent_prompt = Some(message.to_string());
        if let Err(first) = self.fill_and_submit(message, attachments, &baseline_users, remaining_secs()) {
            // Fail closed on anything that might already be in flight.
            let SubmitFailure::BeforeSubmit(why) = first else {
                return Err(first.into_error());
            };
            eprintln!("submit failed: {why:#}");

            // Reconnect: back to our conversation if we have one, otherwise to a
            // fresh chat — nothing has been said yet, so nothing is lost.
            if self.convo_id.is_some() {
                self.reopen_pinned(remaining_secs())
                    .context("could not reattach after a failed submit")?;
            } else {
                self.reopen_fresh(remaining_secs())
                    .context("could not reopen ChatGPT after a failed submit")?;
            }

            // Re-baseline against the reattached page, and skip the resend
            // entirely if the message turns out to be in flight already.
            let users_now = self.user_turns(remaining_secs()).unwrap_or_default();
            if self.convo_id.is_some() && user_turn_rose(&baseline_users, &users_now) {
                eprintln!("the message had already been submitted; observing that turn");
            } else {
                self.fill_and_submit(message, attachments, &users_now, remaining_secs())
                    .map_err(SubmitFailure::into_error)
                    .context("resubmitting after reconnect")?;
                // The poll loop below uses this baseline to tell "our page" from
                // "some other page". A reconnect can land on a chat with FEWER
                // turns than the one we started on (a fresh chat has none), so
                // the pre-reconnect figure would read as a page swap and abort
                // the turn we just successfully submitted.
                baseline_users = users_now;
            }
        }

        // Past this point the prompt is on the server: any failure from here
        // on means an incomplete turn, never "not sent, safe to resend".
        self.submitted = true;
        self.touch_receipt();

        // Re-read the assistant baseline: if we reattached above, the reloaded
        // page reflects the server's view and the pre-crash count is meaningless.
        let baseline_count = baseline_count.min(
            ab_eval(&self.ab, JS_ASSISTANT_COUNT, &self.session, remaining_secs())
                .ok()
                .and_then(|v| v.as_u64())
                .unwrap_or(baseline_count),
        );

        // The page is a PROGRESS signal, never a verdict. It settles in phases —
        // preamble, tool calls, final text — and a mid-turn gap looks exactly
        // like an ending. What closes a turn is the record (`judge_record`);
        // `Watch` only decides when the record is worth asking and whether the
        // page has gone quiet enough to scrape on the turn-one path where no
        // conversation id exists yet.
        let poll_interval = Duration::from_millis(2000);
        let mut last_atext = String::new();
        let mut watch = Watch::new(sopts, Instant::now());

        // Consecutive unreadable polls. ~3 misses (~6s) is well past a normal
        // navigation hiccup and reads as "the tab is gone".
        const LOST_POLLS_BEFORE_REATTACH: u32 = 3;
        let mut lost_polls = 0u32;
        // Consecutive unpinned polls whose user-turn count fell back (see
        // `unpinned_page_lost`).
        let mut unpinned_dips = 0u32;

        // How many times we have backed off for the page's rate-limit dialog.
        let mut limited_waits: u32 = 0;

        // Heartbeat: the page can think silently for minutes, so emit an
        // elapsed-time progress line to stderr (~every 5s) so the wait is visible.
        let started = Instant::now();
        let mut last_beat: u64 = 0;
        eprintln!("[   0.0s] submitted; waiting for reply");

        loop {
            if Instant::now() >= deadline {
                // Before giving up, ask the record. The DOM going quiet is an
                // inference; a closed turn is a fact. Live, a turn that timed
                // out here at 240s had in fact completed — the page had stopped
                // rendering while the model worked fine. Reporting a timeout
                // for an answer that exists is the worst outcome available.
                match self.record_verdict(30.0) {
                    Verdict::Done(answer) => {
                        log_completion("record", &answer, started.elapsed());
                        return self.finish_turn(answer.text, remaining_secs());
                    }
                    Verdict::CutOff { kind, reason, chars } => {
                        return Err(kind.error(reason, chars));
                    }
                    Verdict::Open | Verdict::Unreadable => {}
                }
                // Say what is in the way if something visibly is. A plan or
                // usage limit shows up as a dialog whose wording we cannot
                // trigger on demand to match against, so report its text.
                if let Some(text) = self.blocking_dialog(10.0) {
                    return Err(ChannelError::new(
                        ErrorKind::PageBlocked,
                        format!(
                            "the reply did not complete after {}s and ChatGPT is showing a \
                             dialog: {text:?}",
                            self.timeout_secs
                        ),
                    )
                    .into());
                }
                return Err(ChannelError::new(
                    ErrorKind::Incomplete,
                    format!(
                        "timed out after {}s waiting for ChatGPT to complete the reply",
                        self.timeout_secs
                    ),
                )
                .into());
            }
            nap(poll_interval);
            if cancel_requested() {
                return self.stop_generation(remaining_secs());
            }

            let read = ab_eval(&self.ab, JS_STATE, &self.session, remaining_secs());

            // Is this still OUR conversation? Identity — not "did the eval
            // error?" — is the reliable signal that we lost the page. Closing
            // the tab does NOT surface as a failed eval: chrome-use just hands
            // back a fresh blank session, whose state object reads perfectly
            // well as "no assistant messages, not generating". The old
            // failure-based check therefore never fired and the turn silently
            // spun out the full wall-clock timeout instead of reconnecting.
            let seen_convo = read
                .as_ref()
                .ok()
                .and_then(|v| v.get("convo"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());

            // Id-independent loss check, and the only one that works on turn one:
            // ChatGPT doesn't put /c/<id> in the URL until the FIRST turn is
            // persisted, so mid-turn-one there is no identity to compare. But we
            // hold a submit receipt — the user-turn count rose — and on a short
            // fresh chat that count never legitimately goes DOWN. If it does,
            // we're looking at a different (blank) page.
            //
            // Only before pinning. Once pinned, the URL check below is the
            // authority, and the count is not: a long chat is virtualized, so
            // turns scrolling out of view lower it on a healthy page, and every
            // false "lost" here costs a reattach against the throttle.
            let users_now = read
                .as_ref()
                .ok()
                .and_then(|v| v.get("user_count"))
                .and_then(|v| v.as_u64());

            match (&self.convo_id, &seen_convo) {
                // Latch as soon as the id exists — the server assigns it right
                // after the first submit, so even turn one becomes recoverable
                // rather than having to survive un-pinned until it completes.
                (None, Some(id)) => {
                    eprintln!("pinned to conversation {id}");
                    self.convo_id = Some(id.clone());
                    self.touch_receipt();
                    lost_polls = 0;
                }
                (Some(pinned), Some(seen)) if seen == pinned => lost_polls = 0,
                // Pinned, but the page is showing something else (or nothing).
                // The generation is still running server-side; only our view of
                // it was lost. Reattach and keep observing.
                (Some(_), _) => {
                    lost_polls += 1;
                    if lost_polls >= LOST_POLLS_BEFORE_REATTACH {
                        self.reopen_pinned(remaining_secs())
                            .context("lost the ChatGPT tab and could not reattach")?;
                        lost_polls = 0;
                    }
                    continue;
                }
                (None, None) => {}
            }

            // Checked only now, after a conversation id in the URL had its
            // chance to pin us: a pinned turn is recoverable and the URL check
            // above is its authority.
            if unpinned_page_lost(self.convo_id.is_some(), users_now, baseline_users.count, &mut unpinned_dips) {
                bail!(
                    "the ChatGPT page was replaced while the first turn was still \
                     running, and ChatGPT does not put a conversation id in the URL \
                     until that turn finishes — so there is no conversation to \
                     reattach to. The reply may still have completed in your \
                     browser; rerun the command."
                );
            }
            if unpinned_dips > 0 {
                continue;
            }

            let st = match read {
                Ok(v) if v.is_object() => v,
                _ => continue,
            };

            if st.get("limited").and_then(|v| v.as_bool()).unwrap_or(false) {
                // The prompt is already in. Handing the user an error here throws
                // away a turn the model is very likely still completing — the
                // throttle is on the conversations API, not on generation, which
                // is why the record keeps filling in while the page shows this
                // dialog. So wait it out inside our own deadline instead, and
                // keep asking the server, which is the one source still answering.
                limited_waits += 1;
                if limited_waits == 1 {
                    // This turn waits it out below; later runs must not pile on.
                    crate::throttle::note_rate_limited();
                }
                let backoff = Duration::from_secs(match limited_waits {
                    1 => 20,
                    2 => 45,
                    _ => 90,
                });
                if Instant::now() + backoff >= deadline {
                    return Err(ChannelError::new(
                        ErrorKind::RateLimited,
                        format!(
                            "{} The prompt was already submitted; check the conversation \
                             or retry in a few minutes.",
                            RATE_LIMIT_MSG
                        ),
                    )
                    .into());
                }
                eprintln!(
                    "[{:5}.0s] rate-limited by the page; waiting {}s (attempt {}) — the reply \
                     may still be arriving server-side",
                    started.elapsed().as_secs(),
                    backoff.as_secs(),
                    limited_waits
                );
                // Dismiss the dialog so the page is usable if it does recover.
                let _ = ab_eval(&self.ab, JS_DISMISS_DIALOG, &self.session, remaining_secs());
                nap(backoff);
                match self.record_verdict(remaining_secs().min(30.0)) {
                    Verdict::Done(answer) => {
                        eprintln!("the turn completed despite the throttle; taking it from the record");
                        log_completion("record", &answer, started.elapsed());
                        return self.finish_turn(answer.text, remaining_secs());
                    }
                    Verdict::CutOff { kind, reason, chars } => return Err(kind.error(reason, chars)),
                    Verdict::Open | Verdict::Unreadable => {}
                }
                continue;
            }

            let stop = st.get("stop").and_then(|v| v.as_bool()).unwrap_or(false);
            let tool_active = st.get("tool_active").and_then(|v| v.as_bool()).unwrap_or(false);
            let cur_count = st
                .get("assistant_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            let atext = st
                .get("atext")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let title = st.get("title").and_then(|v| v.as_str()).unwrap_or("");

            // Throttled heartbeat (~every 5s). Logs msg count + reply length so a
            // hang is diagnosable from the log alone (e.g. msgs=0 → no assistant
            // node yet; len frozen → settling; len climbing → still streaming).
            let elapsed = started.elapsed().as_secs();
            if elapsed >= last_beat + 5 {
                last_beat = elapsed;
                let phase = if tool_active {
                    "running a tool"
                } else if stop {
                    "generating"
                } else {
                    "waiting for reply"
                };
                eprintln!("[{elapsed:5}.0s] {phase} (msgs={cur_count}, len={})", atext.len());
            }

            if !atext.is_empty() {
                last_atext = atext.clone();
            }

            // Fold this poll in. `true` means the record is worth asking now.
            // The page going quiet only makes the question cheaper; it can
            // never answer it.
            let due = watch.observe(
                DomProgress {
                    stop,
                    tool_active,
                    // A turn that never actually submitted (Enter swallowed, the
                    // send-button fallback missed) leaves the PREVIOUS reply on
                    // screen, static and stop-button-free. Requiring a new
                    // assistant turn keeps that reply from being read as this
                    // turn's answer.
                    new_turn: cur_count > baseline_count,
                    text: &atext,
                },
                Instant::now(),
            );
            if !due {
                continue;
            }

            match self.record_verdict(remaining_secs().min(30.0)) {
                Verdict::Done(answer) => {
                    log_completion("record", &answer, started.elapsed());
                    return self.finish_turn(answer.text, remaining_secs());
                }
                Verdict::CutOff { kind, reason, chars } => return Err(kind.error(reason, chars)),
                Verdict::Open => watch.record_open(Instant::now()),
                Verdict::Unreadable => {
                    watch.record_unreadable(Instant::now());

                    // The one window with no record to ask: a brand-new chat,
                    // before ChatGPT puts /c/<id> in the URL. Take the page's
                    // copy only when it has settled AND it is a reply rather
                    // than the message chrome. Every pinned turn above waits for
                    // the record instead of falling back to this.
                    if self.convo_id.is_none() && watch.settled() {
                        if let Some(id) = self.current_convo_id(remaining_secs()) {
                            eprintln!("pinned to conversation {id}");
                            self.convo_id = Some(id);
                            self.touch_receipt();
                            continue;
                        }
                        let scraped = ab_eval(
                            &self.ab,
                            JS_LAST_ASSISTANT,
                            &self.session,
                            remaining_secs(),
                        )
                        .ok()
                        .and_then(|v| v.as_str().map(str::to_string))
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| last_atext.clone());
                        if let Some(text) = usable_reply(&scraped, title) {
                            log_scraped_completion(text.chars().count(), started.elapsed());
                            return self.finish_turn(text, remaining_secs());
                        }
                        eprintln!("the page shows no usable reply yet; waiting for the record");
                        // Chrome is not evidence. Start collecting the page's
                        // quiescence again rather than re-scraping every tick.
                        watch.record_open(Instant::now());
                    }
                }
            }
        }
    }

    /// Everything a completed turn owes regardless of HOW it completed — the
    /// record proving it closed, or (turn one, no id yet) a settled page with
    /// a real reply on it. Kept in one place so the two paths cannot drift: a
    /// reply taken from the conversation record must still pin the conversation
    /// and still get filed into its project.
    fn finish_turn(&mut self, reply_text: String, budget: f64) -> Result<String> {
        if reply_text.trim().is_empty() {
            bail!("ChatGPT's reply was empty");
        }

        // Latch identity on the first completed turn: a brand-new chat has no
        // conversation id in its URL until the server persists it, so this is
        // the earliest point we can pin. Every later turn verifies against it.
        if self.convo_id.is_none() {
            if let Some(id) = self.current_convo_id(budget) {
                eprintln!("pinned to conversation {id}");
                self.convo_id = Some(id);
                self.touch_receipt();
            }
        }

        Ok(reply_text)
    }

    /// End the channel. Files any deferred project; keeps the tab (see below).
    ///
    /// Any deferred project filing happens HERE rather than after each turn.
    /// Setting `gizmo_id` makes ChatGPT's client re-route the open conversation
    /// to its project URL, and the composer is gone for the moment that takes —
    /// live, a multi-turn `run` filed after turn one and then failed its next
    /// send with "could not insert text into the ChatGPT composer". A one-shot
    /// `ask` never saw it because it exits immediately after. So we file once,
    /// when nothing is going to type into the page again.
    pub fn close(mut self) {
        if let (Some(gizmo), Some(cid)) = (self.pending_project.clone(), self.convo_id.clone()) {
            match self.file_into_project(&cid, &gizmo, 30.0) {
                Ok(()) => {
                    eprintln!("filed conversation into project {:?}", self.project);
                    self.pending_project = None;
                }
                Err(e) => eprintln!(
                    "warning: could not file the conversation into {:?} ({e}); it stays in a \
                     plain chat",
                    self.project
                ),
            }
        }
        // The tab is deliberately LEFT OPEN. The next run finds it and starts a
        // new chat in place, which costs one backend request; closing it here
        // made every run reopen chatgpt.com from scratch, about 45 requests.
        // Measured: seven runs in a row, all full reloads, none reused, until
        // the account throttle tripped. It is also the one ChatGPT window the
        // shared session is meant to keep.
    }

    /// Navigate the open session's tab to `url` and wait for it to settle.
    /// Used by side flows (e.g. `refresh`) that drive ChatGPT pages other than
    /// the chat composer.
    pub fn open(&self, url: &str) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(self.timeout_secs.min(30));
        ab_open(&self.ab, &self.session, url, None, deadline)
    }

    /// Run JS in the page (the `JSON.stringify(value)` convention) and return the
    /// decoded value. Exposed for side flows like `refresh`.
    pub fn eval(&self, js: &str) -> Result<serde_json::Value> {
        let t = (self.timeout_secs.min(30) as f64).max(5.0);
        ab_eval(&self.ab, js, &self.session, t)
    }

    /// Navigate the open session into the named ChatGPT Project, creating it on
    /// first use. Mirrors `_enter_project` in chatgpt-imagegen.
    fn resolve_project(&self, name: &str, deadline: Instant) -> Result<(String, bool)> {
        let remaining = || {
            deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::from_secs(2))
                .as_secs_f64()
                .max(2.0)
        };

        // A remembered id skips the account-wide project listing entirely.
        if let Some(id) = cached_gizmo(name) {
            return Ok((id, false));
        }

        let js = js_ensure_project(name);
        let res = ab_eval(&self.ab, &js, &self.session, remaining().min(30.0))
            .context("resolving ChatGPT Project")?;

        let ok = res.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
        if !ok {
            let detail = res
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            bail!("could not resolve project: {detail}");
        }

        let gizmo_id = res
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("project resolve returned no id"))?;

        let created = res
            .get("created")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        remember_gizmo(name, gizmo_id);
        Ok((gizmo_id.to_string(), created))
    }

    /// Navigate into the project's page so the next message is composed inside it.
    ///
    /// This is the preferred route — a conversation born in the project needs no
    /// repair afterwards — but it depends on ChatGPT's project page rendering,
    /// which is not something we control. See `file_into_project` for what
    /// happens when it doesn't.
    fn open_project_page(&self, gizmo_id: &str, deadline: Instant) -> Result<()> {
        let remaining = || {
            deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::from_secs(2))
                .as_secs_f64()
                .max(2.0)
        };
        // Sidebar link first, for the same reason as everywhere else: the SPA
        // route costs a fraction of a reload, and this navigation happens on
        // every run that names a project.
        let in_place = ab_eval(
            &self.ab,
            &js_open_project_in_place(gizmo_id),
            &self.session,
            remaining().min(15.0),
        )
        .ok()
        .and_then(|v| v.get("ok").and_then(|b| b.as_bool()))
        .unwrap_or(false);
        if !in_place {
            let project_url = WEB_PROJECT_URL_TPL.replace("{gizmo_id}", gizmo_id);
            ab_open(&self.ab, &self.session, &project_url, None, deadline)?;
        }

        // Wait until the SPA has ACTUALLY settled into the project — not just that
        // a `#prompt-textarea` exists. `chrome-use open` can return while the prior
        // top-level page (https://chatgpt.com/) is still showing its composer; a
        // bare composer check passes against THAT, and the next send() then submits
        // on the top-level context, filing the conversation OUTSIDE the project.
        // Gate on the URL containing the project gizmo id so we type into the real
        // project composer. (The project URL is /g/<gizmo_id>/project and stays on
        // that gizmo path until submit, so a substring check is reliable.)
        let js_project_ready = format!(
            r#"(() => {{{helper}
  const gid = {gid};
  return JSON.stringify({{
    composer: !!__cguComposer(),
    in_project: (location.href || '').includes(gid),
  }});
}})()"#,
            gid = serde_json::to_string(gizmo_id).unwrap_or_else(|_| "\"\"".to_string()),
            helper = js_composer_helper!(),
        );
        let mut ready = false;
        for _ in 0..30 {
            if Instant::now() >= deadline {
                break;
            }
            if let Ok(st) = ab_eval(&self.ab, &js_project_ready, &self.session, remaining().min(20.0)) {
                let composer = st.get("composer").and_then(|v| v.as_bool()).unwrap_or(false);
                let in_project = st.get("in_project").and_then(|v| v.as_bool()).unwrap_or(false);
                if composer && in_project {
                    ready = true;
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        if !ready {
            bail!("project page never settled (composer + project URL) within the deadline");
        }

        Ok(())
    }

    /// Set the composer's model, verified against the live UI on 2026-09-10.
    ///
    /// ChatGPT rebuilt this control and the old "click the menu item named Pro"
    /// approach cannot work any more. What is there now:
    ///
    ///   * The picker BUTTON's text is not stable and must never be matched on.
    ///     Observed on one account inside three weeks: "Instant", "5.6 SolLight",
    ///     "6Pro" — and "Thinking effort" while its own menu is open. Those come
    ///     from a model catalogue whose `shortLabel` changes with the model
    ///     line-up. We locate it structurally instead: the one
    ///     `button[aria-haspopup="menu"]` sharing the composer toolbar row with
    ///     `[data-testid="composer-plus-btn"]` (excluding the plus button).
    ///   * The five intelligence levels are NO LONGER menu items. They are a
    ///     `[role="slider"]` with `aria-valuenow` 0..4 — Instant, Medium, High,
    ///     Extra High, Pro — driven with arrow keys. So we verify on the INDEX,
    ///     which is stable, never on the rendered name, which is not.
    ///   * Model family is a separate axis: `[role="menuitemradio"]` entries
    ///     ("Latest", "GPT-5.6 Sol", "GPT-5.5"). A `--model` that isn't a level
    ///     name is matched against those.
    ///
    /// Two clicks must be REAL input events, not `element.click()`: opening the
    /// picker, and focusing the slider thumb. JS clicks are ignored by both.
    fn select_model(&self, model: &str, deadline: Instant) -> Result<()> {
        let remaining = || {
            deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::from_secs(2))
                .as_secs_f64()
                .max(2.0)
        };

        let want = model.trim().to_lowercase();
        let want_level = level_index(&want);

        // 1. Locate the picker structurally and open it with a real click.
        let mut pick = serde_json::Value::Null;
        for attempt in 0..6 {
            pick = ab_eval(&self.ab, JS_FIND_PICKER, &self.session, remaining())?;
            if pick.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                break;
            }
            if attempt < 5 {
                std::thread::sleep(Duration::from_millis(600));
            }
        }
        if !pick.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
            let detail = pick.get("error").and_then(|v| v.as_str()).unwrap_or("unknown");
            bail!("could not find the composer model picker: {detail}");
        }
        let (px, py) = match (
            pick.get("x").and_then(|v| v.as_i64()),
            pick.get("y").and_then(|v| v.as_i64()),
        ) {
            (Some(x), Some(y)) => (x, y),
            _ => bail!("composer model picker has no usable coordinates"),
        };
        ab_cmd(&self.ab, &["click", &px.to_string(), &py.to_string()], &self.session, remaining())
            .context("opening the composer model picker")?;
        std::thread::sleep(Duration::from_millis(500));

        let st = ab_eval(&self.ab, JS_PICKER_MENU, &self.session, remaining())?;
        if !st.get("open").and_then(|v| v.as_bool()).unwrap_or(false) {
            let via = pick.get("via").and_then(|v| v.as_str()).unwrap_or("?");
            bail!(
                "clicked the model picker (found by {via}) but its menu did not open; pass \
                 --model current to use the account's current model"
            );
        }

        let outcome = match want_level {
            Some(idx) => self.set_level(&st, idx, &remaining),
            None => self.set_model_family(&st, model.trim(), &remaining),
        };

        // Close the menu whether or not we succeeded, so the composer is usable.
        let _ = ab_cmd(&self.ab, &["press", "Escape"], &self.session, remaining());
        outcome
    }

    /// Walk the thinking-effort slider to `idx` with arrow keys.
    fn set_level(
        &self,
        st: &serde_json::Value,
        idx: usize,
        remaining: &dyn Fn() -> f64,
    ) -> Result<()> {
        let slider = st.get("slider").filter(|v| !v.is_null()).ok_or_else(|| {
            anyhow!(
                "the composer menu has no thinking-effort slider — ChatGPT has \\
                 changed this control again. Rerun without --model to use the \\
                 account default."
            )
        })?;
        let now = slider.get("now").and_then(|v| v.as_i64()).unwrap_or(-1);
        let max = slider.get("max").and_then(|v| v.as_i64()).unwrap_or(-1);
        let last = (LEVEL_ORDER.len() - 1) as i64;
        if max != last {
            bail!(
                "the thinking-effort slider now has {} positions but this build \\
                 knows {} levels ({}) — refusing to guess which is which",
                max + 1,
                LEVEL_ORDER.len(),
                LEVEL_ORDER.join(", ")
            );
        }
        let (tx, ty) = match (
            slider.get("thumbX").and_then(|v| v.as_i64()),
            slider.get("thumbY").and_then(|v| v.as_i64()),
        ) {
            (Some(x), Some(y)) => (x, y),
            _ => bail!("the thinking-effort slider has no usable thumb coordinates"),
        };

        // A real click on the thumb; this focuses the slider WITHOUT closing the
        // menu, which `press --selector` does not manage (focusing through a
        // selector dismisses the popover and the arrow keys go nowhere).
        ab_cmd(&self.ab, &["click", &tx.to_string(), &ty.to_string()], &self.session, remaining())
            .context("focusing the thinking-effort slider")?;
        std::thread::sleep(Duration::from_millis(300));

        let target = idx as i64;
        let key = if target >= now { "ArrowRight" } else { "ArrowLeft" };
        for _ in 0..(target - now).abs() {
            ab_cmd(&self.ab, &["press", key], &self.session, remaining())
                .context("moving the thinking-effort slider")?;
            std::thread::sleep(Duration::from_millis(250));
        }
        std::thread::sleep(Duration::from_millis(300));

        let after = ab_eval(&self.ab, JS_PICKER_MENU, &self.session, remaining())?;
        let got = after
            .get("slider")
            .and_then(|v| v.get("now"))
            .and_then(|v| v.as_i64())
            .unwrap_or(-1);
        if got != target {
            bail!(
                "could not move the thinking-effort slider to {} ({}): it sits at {}",
                LEVEL_ORDER[idx],
                target,
                got
            );
        }
        let shown = after.get("level").and_then(|v| v.as_str()).unwrap_or("");
        eprintln!("model: {} (slider {}{})", LEVEL_ORDER[idx], target,
            if shown.is_empty() { String::new() } else { format!(", shown as {shown:?}") });
        Ok(())
    }

    /// Pick a model family (`Latest`, `GPT-5.5`, …) from the menu's radio items.
    fn set_model_family(
        &self,
        st: &serde_json::Value,
        want: &str,
        remaining: &dyn Fn() -> f64,
    ) -> Result<()> {
        let empty = vec![];
        let radios = st.get("radios").and_then(|v| v.as_array()).unwrap_or(&empty);
        let names: Vec<String> = radios
            .iter()
            .filter_map(|r| r.get("text").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();
        let wl = want.to_lowercase();
        let hit = names.iter().find(|n| n.to_lowercase() == wl).cloned().or_else(|| {
            names.iter().find(|n| n.to_lowercase().contains(&wl)).cloned()
        });
        let Some(hit) = hit else {
            bail!(
                "{want:?} is neither a thinking-effort level ({}) nor one of the \\
                 models this account offers ({})",
                LEVEL_ORDER.join(", "),
                if names.is_empty() { "none listed".to_string() } else { names.join(", ") }
            );
        };

        let res = ab_eval(&self.ab, &js_click_radio(&hit), &self.session, remaining())?;
        if !res.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
            bail!("could not select model {hit:?}");
        }
        eprintln!("model: {hit}");
        Ok(())
    }

}

// ---- chrome-use helpers (mirrors _ab / _ab_eval in chatgpt-imagegen) --------

/// Locate the chrome-use binary: search PATH, then ~/.local/bin.
/// Where a turn died, so the caller knows whether retrying could duplicate it.
///
/// This is the "ambiguous submit" distinction: recovering from a lost tab is
/// only safe while nothing has been sent. Once Enter has been pressed we may be
/// looking at a message that IS generating server-side but whose receipt we
/// never saw — resending it would post the prompt twice.
enum SubmitFailure {
    /// Failed before anything was submitted — safe to reconnect and retry.
    BeforeSubmit(anyhow::Error),
    /// Enter was pressed but no receipt appeared — never auto-retry.
    Ambiguous(anyhow::Error),
}

impl SubmitFailure {
    fn into_error(self) -> anyhow::Error {
        match self {
            SubmitFailure::BeforeSubmit(e) => e,
            // Whatever the proximate cause, the prompt may be on the server and
            // the caller must not resend it.
            SubmitFailure::Ambiguous(mut e) => {
                if let Some(ce) = e.downcast_mut::<ChannelError>() {
                    ce.submitted = Submitted::Unknown;
                    return e;
                }
                e.context(
                    ChannelError::new(ErrorKind::SubmitUnknown, "the prompt may or may not have been sent")
                        .with_submitted(Submitted::Unknown),
                )
            }
        }
    }
}

/// One node of the live turn, as the conversation record states it.
///
/// Parsed at the boundary ([`RecordTurn::from_json`]), so the judgement works
/// on typed values and `null` from the page is already normalised here.
#[derive(Debug, Clone, Default, PartialEq)]
struct RecordNode {
    id: String,
    role: String,
    content_type: String,
    /// `message.channel`: "final" is the public answer; "commentary" and
    /// "analysis" are steps that are not the answer.
    channel: String,
    recipient: String,
    weight: f64,
    end_turn: bool,
    /// `message.status` and the node's own `status`, kept apart: either one
    /// saying `in_progress` means the turn is still being written.
    status: String,
    node_status: String,
    is_complete: bool,
    hidden: bool,
    /// `metadata.finish_details.type`: "stop" for a real finish.
    finish: Option<String>,
    reason: Option<String>,
    text: String,
}

impl RecordNode {
    fn from_json(v: &serde_json::Value) -> Self {
        let s = |k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
        let optional = |k: &str| {
            let s = s(k);
            if s.is_empty() {
                None
            } else {
                Some(s)
            }
        };
        RecordNode {
            id: s("id"),
            role: s("role"),
            content_type: s("content_type"),
            channel: s("channel"),
            recipient: s("recipient"),
            weight: v.get("weight").and_then(|x| x.as_f64()).unwrap_or(1.0),
            end_turn: v.get("end_turn").and_then(|x| x.as_bool()).unwrap_or(false),
            status: s("status"),
            node_status: s("node_status"),
            is_complete: v
                .get("is_complete")
                .and_then(|x| x.as_bool())
                .unwrap_or(true),
            hidden: v.get("hidden").and_then(|x| x.as_bool()).unwrap_or(false),
            finish: optional("finish"),
            reason: optional("reason"),
            text: s("text"),
        }
    }

    fn is_assistant(&self) -> bool {
        self.role == "assistant"
    }

    fn has_text(&self) -> bool {
        !self.text.trim().is_empty()
    }

    fn is_in_progress(&self) -> bool {
        self.status == "in_progress" || self.node_status == "in_progress"
    }
}

/// The record's view of the current turn: every node from the newest back to
/// (but not including) the latest user message, plus the identity fields.
#[derive(Debug, Clone, Default, PartialEq)]
struct RecordTurn {
    conversation_id: String,
    user_text: Option<String>,
    /// Newest first.
    nodes: Vec<RecordNode>,
}

impl RecordTurn {
    fn from_json(v: &serde_json::Value) -> Option<Self> {
        Some(RecordTurn {
            conversation_id: v
                .get("conversation_id")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            user_text: v
                .get("user_text")
                .and_then(|x| x.as_str())
                .map(str::to_string),
            nodes: v
                .get("nodes")?
                .as_array()?
                .iter()
                .map(RecordNode::from_json)
                .collect(),
        })
    }

    /// Does the record say which messages are public answers? Without a
    /// `channel` anywhere, a step and the answer look identical, so the record
    /// proves nothing (see [`judge_record`]).
    fn marks_channels(&self) -> bool {
        self.nodes.iter().any(|n| !n.channel.is_empty())
    }
}

/// A reply the record proves finished.
#[derive(Debug, Clone, PartialEq)]
struct Answer {
    text: String,
    message_id: String,
    end_turn: bool,
    finish: Option<String>,
    content_type: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CutOffKind {
    Interrupted,
    MaxTokens,
}

impl CutOffKind {
    fn error(self, reason: Option<String>, chars: usize) -> anyhow::Error {
        let why = match (self, reason.as_deref()) {
            (CutOffKind::MaxTokens, _) => "hit its length limit and was cut off".to_string(),
            (CutOffKind::Interrupted, Some(reason)) => format!("was interrupted ({reason})"),
            (CutOffKind::Interrupted, None) => "was interrupted".to_string(),
        };
        ChannelError::new(
            ErrorKind::Incomplete,
            format!(
                "ChatGPT's reply {why} before it finished; the {chars} characters so far are \
                 partial, so they are not returned as a reply"
            ),
        )
        .with_submitted(Submitted::Yes)
        .into()
    }
}

/// What one record read proves about the turn.
#[derive(Debug, Clone, PartialEq)]
enum Verdict {
    Done(Answer),
    /// Readable, and the turn has not closed.
    Open,
    /// Closed early by a stop or the length limit.
    CutOff {
        kind: CutOffKind,
        reason: Option<String>,
        chars: usize,
    },
    /// No proof: the read failed, or the record is not this turn.
    Unreadable,
}

/// Is this node a candidate for the public answer? Only the `final` channel
/// carries the answer; `commentary`/`analysis` are steps, and a record that
/// marks no channel at all is judged open before this is asked.
fn is_answer(n: &RecordNode) -> bool {
    n.is_assistant()
        && !n.hidden
        && n.weight != 0.0
        && (n.recipient.is_empty() || n.recipient == "all")
        && matches!(n.content_type.as_str(), "" | "text" | "multimodal_text")
        && n.channel == "final"
        && n.has_text()
}

/// A node that may legitimately sit AFTER the answer: the "Thought for Xs"
/// disclosure ChatGPT writes once the answer is done, or a structural node
/// with nothing on it. Hidden nodes never reach this check. Anything else
/// newer than the answer means the turn kept writing, so that message was a
/// step, not the reply.
fn is_afterword(n: &RecordNode) -> bool {
    n.role.is_empty()
        || n.content_type == "reasoning_recap"
        || (n.weight == 0.0 && !n.has_text())
}

fn is_cut_off(n: &RecordNode) -> bool {
    matches!(n.finish.as_deref(), Some("interrupted" | "max_tokens"))
}

/// Judge a record read. A stopped turn is closed like a finished one
/// (`end_turn`, `finished_successfully`, `is_complete` all true) and only
/// `finish_details` sets it apart: observed live, `{"type":"interrupted",
/// "reason":"client_stopped"}` against `{"type":"stop"}`. Its text is partial,
/// so it is an incomplete turn, never a reply. Only KNOWN cut-offs count
/// against a reply; an unrecognised finish type passes as a normal finish, so
/// a model that labels its stop differently keeps working.
///
/// `Done` needs proof, not a plausible message: the newest assistant answer is
/// the newest thing the turn wrote (apart from the reasoning disclosure), that
/// message says `end_turn`, it finished successfully, and nothing above it is
/// still being written. A per-message `end_turn` alone is not proof — a
/// multi-step turn closes each STEP, and taking one of those as the answer is
/// how a `completed` envelope beat the real reply.
fn judge_record(turn: &RecordTurn) -> Verdict {
    // `hidden` messages are not rendered: a regenerated tail, tool plumbing.
    // They are not part of the conversation the user sees, so they are neither
    // the answer nor newer writing that keeps a visible answer open. Measured
    // live: a long answer was regenerated and the record kept the hidden tail
    // ABOVE the visible complete answer, which held the turn open forever.
    let nodes: Vec<&RecordNode> = turn.nodes.iter().filter(|n| !n.hidden).collect();

    if let Some((i, n)) = nodes.iter().enumerate().find(|(_, n)| is_cut_off(n)) {
        // A marker a later visible message already superseded (an interrupted
        // thinking step, then a finished answer) is not this turn's ending.
        let superseded = nodes[..i]
            .iter()
            .any(|newer| newer.is_assistant() && newer.has_text());
        if !superseded {
            return Verdict::CutOff {
                kind: if n.finish.as_deref() == Some("max_tokens") {
                    CutOffKind::MaxTokens
                } else {
                    CutOffKind::Interrupted
                },
                reason: n.reason.clone(),
                chars: nodes[..=i]
                    .iter()
                    .find(|x| x.has_text())
                    .map(|x| x.text.chars().count())
                    .unwrap_or(0),
            };
        }
    }

    // Without a `channel` anywhere there is no way to tell a public answer
    // from a step: a multi-step turn closes each step with `end_turn` too, so
    // the message's own flags cannot prove the turn is over. (A record with no
    // assistant message yet is the same case.) Keep waiting; the caller turns
    // an expired wait into an incomplete turn rather than a reply.
    if !turn.marks_channels() {
        return Verdict::Open;
    }

    let Some(idx) = nodes.iter().position(|n| is_answer(n)) else {
        return Verdict::Open;
    };
    let answer = nodes[idx];

    // The answer must be the newest thing the turn wrote.
    if !nodes[..idx].iter().all(|n| is_afterword(n)) {
        return Verdict::Open;
    }
    // Anything still being written ABOVE the answer keeps the turn open,
    // whatever the answer's own flags say. Nodes below it are older history:
    // a stale `in_progress` there cannot be written again once a newer answer
    // closed, and scanning them would hold a finished turn open until timeout.
    if nodes[..=idx].iter().any(|n| n.is_in_progress()) {
        return Verdict::Open;
    }
    if !answer.end_turn
        || !answer.is_complete
        || !matches!(answer.status.as_str(), "" | "finished_successfully")
    {
        return Verdict::Open;
    }
    let text = strip_private_markers(answer.text.trim());
    if text.trim().is_empty() {
        return Verdict::Open;
    }
    Verdict::Done(Answer {
        text,
        message_id: answer.id.clone(),
        end_turn: answer.end_turn,
        finish: answer.finish.clone(),
        content_type: answer.content_type.clone(),
    })
}

/// Does this record describe the turn we are waiting on? A conversation id or
/// user prompt that disagrees means no verdict may be drawn from it.
fn record_is_ours(
    turn: &RecordTurn,
    convo_id: Option<&str>,
    prompt: Option<&str>,
    connector: bool,
) -> bool {
    if let Some(want) = convo_id.filter(|s| !s.is_empty()) {
        if !turn.conversation_id.is_empty() && turn.conversation_id != want {
            return false;
        }
    }
    match (prompt, turn.user_text.as_deref()) {
        (Some(want), Some(got)) => same_prompt(want, got, connector),
        // The record does not say which user turn this is. That is not
        // permission to assume it is ours: the previous turn's answer is
        // closed and sits in the same conversation, so accepting it would
        // return an old reply for a new prompt. Wait for a record that names
        // the turn.
        (Some(_), None) => false,
        // No prompt to compare (resume attaches).
        (None, _) => true,
    }
}

/// Compare a submitted prompt with the record's copy. The record may normalize
/// whitespace, a connector mention is prepended as a pill, and a very long
/// prompt need not survive verbatim — so a long prompt is identified by its
/// opening and a connector turn by its tail.
fn same_prompt(expected: &str, got: &str, connector: bool) -> bool {
    let e = normalize_space(expected);
    let g = normalize_space(got);
    if e == g {
        return true;
    }
    if connector && g.ends_with(&e) {
        return true;
    }
    let head: String = e.chars().take(200).collect();
    e.chars().count() > 200 && g.contains(&head)
}

fn normalize_space(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The words ChatGPT's own message chrome renders as text.
const CHROME_WORDS: &[&str] = &[
    "chatgpt", "gpt", "assistant", "you", "said", "says", "답변", "응답", "回答", "答復", "回复",
    "回應", "响应",
];

/// Split off the first word, where a "word" is a run of letters or digits in
/// any script. Korean/Chinese labels are single words by this rule.
fn split_word(s: &str) -> Option<(&str, &str)> {
    let s = s.trim_start();
    let end = s
        .char_indices()
        .find(|(_, c)| !c.is_alphanumeric())
        .map(|(i, _)| i)
        .unwrap_or(s.len());
    if end == 0 {
        return None;
    }
    Some((&s[..end], &s[end..]))
}

fn is_chrome_word(w: &str) -> bool {
    CHROME_WORDS.iter().any(|c| w.eq_ignore_ascii_case(c))
}

/// `Some(rest)` when the text opens with a colon-terminated chrome header
/// ("ChatGPT 답변:", "ChatGPT said:"), `None` for ordinary prose. The colon is
/// what separates a header from a sentence that merely starts with a label
/// word ("You can ...").
fn chrome_header_rest(s: &str) -> Option<&str> {
    let mut rest = s.trim_start();
    loop {
        let (word, tail) = split_word(rest)?;
        if !is_chrome_word(word) {
            return None;
        }
        rest = tail.trim_start();
        if let Some(after) = rest.strip_prefix(':').or_else(|| rest.strip_prefix('：')) {
            return Some(after);
        }
    }
}

/// True when the text is nothing but chrome labels and punctuation.
fn chrome_only(s: &str) -> bool {
    let mut rest = s.trim_start();
    loop {
        match split_word(rest) {
            None => return true,
            Some((word, tail)) => {
                if !is_chrome_word(word) {
                    return false;
                }
                rest = tail.trim_start().trim_start_matches([':', '：']).trim_start();
            }
        }
    }
}

/// Is `text` a reply a reader could have seen, rather than a piece of ChatGPT's
/// own chrome? The assistant header renders as `sr-only` text, so a message
/// with no content yet scrapes as its own label ("ChatGPT 답변:ChatGPT"). That
/// is the exact string that reached a `completed` envelope.
fn usable_reply(text: &str, page_title: &str) -> Option<String> {
    let mut s = text.trim();
    if let Some(rest) = chrome_header_rest(s) {
        s = rest.trim();
    }
    if s.is_empty() || chrome_only(s) {
        return None;
    }
    // The tab title is the chat's name, never its reply. A scrape that equals
    // it came from the wrong element.
    let title = page_title.trim();
    if !title.is_empty() && s.eq_ignore_ascii_case(title) {
        return None;
    }
    Some(s.to_string())
}

/// One poll's view of the page. Progress only: none of this can close a turn.
#[derive(Debug, Clone, Copy)]
struct DomProgress<'a> {
    stop: bool,
    tool_active: bool,
    /// A new assistant turn exists (the count rose above the baseline).
    new_turn: bool,
    text: &'a str,
}

/// No record read before this long after a readable "still open" answer.
const RECORD_RECHECK_STREAMING: Duration = Duration::from_secs(20);
/// …and this long after one when the page looks settled, which is when the
/// turn is most likely to have just closed.
const RECORD_RECHECK_SETTLED: Duration = Duration::from_secs(5);
const RECORD_BACKOFF_MIN: Duration = Duration::from_secs(2);
const RECORD_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// The completion state machine for one turn.
///
/// It owns exactly two questions: has the page gone quiet enough that the
/// record is worth asking, and has the record been asked recently enough. The
/// answer to "is the turn finished" never lives here — see [`judge_record`].
/// Keeping the counters in one tested place is what stops a settled page from
/// quietly becoming a completed turn again.
struct Watch {
    stable_needed: u32,
    idle_limit: u32,
    prev_text: String,
    stable_polls: u32,
    idle_polls: u32,
    /// No record read before this instant.
    next_record: Instant,
    /// Current wait after an unreadable read.
    backoff: Duration,
}

impl Watch {
    fn new(opts: &SendOptions, now: Instant) -> Self {
        Watch {
            stable_needed: opts.stable_needed.max(1),
            idle_limit: opts.idle_limit.max(1),
            prev_text: String::new(),
            stable_polls: 0,
            idle_polls: 0,
            next_record: now,
            backoff: RECORD_BACKOFF_MIN,
        }
    }

    /// Fold one poll in. `true` means the record should be asked now.
    fn observe(&mut self, dom: DomProgress, now: Instant) -> bool {
        if dom.stop || dom.tool_active || !dom.new_turn {
            self.stable_polls = 0;
            self.idle_polls = 0;
            self.prev_text.clear();
        } else {
            if !dom.text.is_empty() && dom.text == self.prev_text {
                self.stable_polls += 1;
            } else {
                self.prev_text = dom.text.to_string();
                self.stable_polls = 0;
            }
            // Never reset: a reply whose innerText keeps mutating (collapsible
            // tool disclosures) must still reach the record eventually.
            self.idle_polls += 1;
        }
        now >= self.next_record
    }

    /// The page has gone quiet with a new assistant turn on it.
    fn settled(&self) -> bool {
        self.stable_polls >= self.stable_needed || self.idle_polls >= self.idle_limit
    }

    /// The record says the turn is still open: the page's quiescence is spent
    /// evidence, so start collecting it again.
    fn record_open(&mut self, now: Instant) {
        let settled = self.settled();
        self.stable_polls = 0;
        self.idle_polls = 0;
        self.backoff = RECORD_BACKOFF_MIN;
        self.next_record = now
            + if settled {
                RECORD_RECHECK_SETTLED
            } else {
                RECORD_RECHECK_STREAMING
            };
    }

    /// The record could not be read: back off, bounded, so a throttled or
    /// broken record endpoint is retried without hammering it.
    fn record_unreadable(&mut self, now: Instant) {
        self.next_record = now + self.backoff;
        self.backoff = (self.backoff * 2).min(RECORD_BACKOFF_MAX);
    }
}

/// Report HOW a turn completed, never WHAT it said: enough to diagnose a wrong
/// completion from a log alone, and nothing private.
fn log_completion(source: &str, answer: &Answer, elapsed: Duration) {
    let content_type = if answer.content_type.is_empty() {
        "-"
    } else {
        &answer.content_type
    };
    eprintln!(
        "completion: source={source} message_id={} end_turn={} finish_details={} content_type={} \
         text_len={} elapsed_ms={}",
        answer.message_id,
        answer.end_turn,
        answer.finish.as_deref().unwrap_or("-"),
        content_type,
        answer.text.chars().count(),
        elapsed.as_millis(),
    );
}

fn log_scraped_completion(text_len: usize, elapsed: Duration) {
    eprintln!(
        "completion: source=dom_unpinned message_id=- end_turn=- finish_details=- content_type=text \
         text_len={text_len} elapsed_ms={}",
        elapsed.as_millis(),
    );
}

/// Cross-process lock on the shared ChatGPT web surface.
///
/// The surface is concurrency-1: one logged-in tab, and an account that
/// rate-limits hard. Two processes driving it interleave inside a single
/// composer — B's clear-and-insert lands on top of A's half-written prompt —
/// which live-reproduced as both aborting with "42 non-whitespace chars
/// present, 21 expected", i.e. two prompts concatenated.
///
/// Held for the whole CHANNEL — connect through close — not per turn. Per-turn
/// was enough while every process opened its own tab, but once they share one
/// window (see DEFAULT_SESSION) `connect` itself is destructive: it navigates
/// the shared tab to a new chat. Live: a second `ask` starting mid-turn blew
/// away the first one's page (the first survived only because it had pinned its
/// conversation and could reattach), and then failed itself because the tab had
/// moved on again by the time it got the lock. Whoever holds the surface owns
/// the tab for as long as it needs it.
///
/// Verified with two concurrent `ask` runs: the second prints its wait line
/// BEFORE "opening ChatGPT", i.e. it blocks before touching the browser at all,
/// and both turns then complete with neither having to reattach.
struct SurfaceLock {
    _file: Option<File>,
}

impl SurfaceLock {
    /// Block until this process owns the surface, best-effort.
    ///
    /// If the lock file can't be created (no home directory, read-only home), run without
    /// it and say so: refusing to work because we couldn't take an advisory lock
    /// would be worse than the race it guards.
    fn acquire(fail_fast: bool) -> Result<Self> {
        let Some(path) = lock_path() else {
            return Ok(SurfaceLock { _file: None });
        };
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        // NOT opened in append mode. The holder line is rewritten in place at
        // offset 0, and with O_APPEND every write silently goes to end-of-file
        // and the seek is ignored — which would leave the PREVIOUS holder's line
        // first and make every waiter name the wrong process. Not truncated
        // either: truncating a locked file is a sharing violation on Windows,
        // and since our line ends in "\n" any longer remnant lands on line 2,
        // which the reader ignores.
        let mut file = match std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
        {
            Ok(f) => f,
            Err(e) => {
                eprintln!("warning: no channel lock ({e}); concurrent runs may collide");
                return Ok(SurfaceLock { _file: None });
            }
        };

        // Announce a wait rather than appearing to hang: a queued turn can sit
        // here for as long as the turn ahead of it takes — an image generation
        // holds it for a minute or more — and "waiting" with no subject reads as
        // "stuck".
        if file.try_lock().is_err() {
            let who = holder_label(&std::fs::read_to_string(&path).unwrap_or_default());
            if fail_fast {
                return Err(ChannelError::new(
                    ErrorKind::Busy,
                    format!("{who} is using ChatGPT; not waiting (--busy fail)"),
                )
                .into());
            }
            eprintln!("waiting for {who} to finish with ChatGPT…");
            // Poll rather than block, so a queued request can be cancelled.
            loop {
                match file.try_lock() {
                    Ok(()) => break,
                    Err(std::fs::TryLockError::WouldBlock) => {}
                    Err(std::fs::TryLockError::Error(e)) => {
                        eprintln!("warning: could not take the channel lock ({e}); proceeding");
                        return Ok(SurfaceLock { _file: None });
                    }
                }
                if cancel_requested() {
                    return Err(ChannelError::new(
                        ErrorKind::Cancelled,
                        "cancelled while queued for the ChatGPT window; nothing was sent",
                    )
                    .into());
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        }

        // Claim it by name so the next waiter can say who it is waiting for.
        // Best-effort: a lock we hold but could not stamp is still a good lock.
        let _ = file
            .seek(std::io::SeekFrom::Start(0))
            .and_then(|_| file.write_all(format!("chatgpt-use {}\n", std::process::id()).as_bytes()))
            .and_then(|_| file.flush());

        Ok(SurfaceLock { _file: Some(file) })
    }
}

/// Strip ChatGPT's private-use control markers from a record-sourced reply.
///
/// The conversation record is the true text, which turns out to include markup
/// the UI never shows: citation spans delimited by U+E200/U+E201 with U+E202
/// separators, e.g. `\u{e200}cite\u{e202}turn0search4\u{e201}`, plus occasional
/// bare private-use characters. Scraping innerText hid these because the page
/// renders them as links; reading the record does not, so a downstream consumer
/// — Claude Code, through `serve` — would receive them verbatim.
///
/// Whole delimited spans go, since their contents are internal ids rather than
/// anything a reader wants; stray private-use characters go too.
fn strip_private_markers(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut depth = 0usize;
    for c in text.chars() {
        match c {
            '\u{e200}' => depth += 1,
            '\u{e201}' => depth = depth.saturating_sub(1),
            _ if depth > 0 => {}
            // Bare private-use characters outside a span carry no meaning either.
            '\u{e000}'..='\u{f8ff}' => {}
            _ => out.push(c),
        }
    }
    // Collapse the blank lines a removed trailing span can leave behind.
    while out.contains("\n\n\n") {
        out = out.replace("\n\n\n", "\n\n");
    }
    out.trim_end().to_string()
}

/// Rust twin of `JS_COMPOSER_FINGERPRINT`: non-whitespace count plus an
/// order-sensitive FNV-1a hash over UTF-16 code units.
///
/// Must stay byte-for-byte equivalent to the JS. The whitespace set is spelled
/// out for the same reason it is there: `char::is_whitespace` and JavaScript's
/// `\s` classify U+0085 and U+FEFF differently, and disagreeing by one
/// character fails an otherwise perfect 30 KB prompt.
fn composer_fingerprint(text: &str) -> (u64, u32) {
    fn is_wire_ws(u: u16) -> bool {
        matches!(u,
            0x09..=0x0d | 0x20 | 0x85 | 0xa0 | 0x1680 | 0x2000..=0x200a
            | 0x2028 | 0x2029 | 0x202f | 0x205f | 0x3000 | 0xfeff)
    }
    let mut n: u64 = 0;
    let mut h: u32 = 0x811c_9dc5;
    for u in text.encode_utf16() {
        if is_wire_ws(u) {
            continue;
        }
        n += 1;
        h ^= u as u32 & 0xff;
        h = h.wrapping_mul(0x0100_0193);
        h ^= (u >> 8) as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    (n, h)
}

/// Remembered project name -> gizmo id, at `~/.chatgpt-use/projects.json`.
///
/// Resolving a project costs a request that lists every project on the account,
/// on every single connect, to learn something that essentially never changes.
/// Against a throttle that counts requests, that is worth caching. Purely an
/// optimisation: any read or write failure just means we resolve the slow way.
fn project_cache_path() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(crate::platform::home_dir)
        .map(|h| h.join(".chatgpt-use").join("projects.json"))
}

fn read_project_cache() -> serde_json::Map<String, serde_json::Value> {
    project_cache_path()
        .and_then(|p| std::fs::read_to_string(p).ok())
        .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default()
}

fn cached_gizmo(name: &str) -> Option<String> {
    read_project_cache()
        .get(name)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn remember_gizmo(name: &str, gizmo_id: &str) {
    let Some(path) = project_cache_path() else { return };
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let mut map = read_project_cache();
    map.insert(name.to_string(), serde_json::Value::String(gizmo_id.to_string()));
    if let Ok(text) = serde_json::to_string_pretty(&serde_json::Value::Object(map)) {
        let _ = std::fs::write(path, text);
    }
}

/// Drop a remembered id after it turns out not to work — a deleted project, or
/// one that belongs to a different account than the browser is now signed into.
fn forget_gizmo(name: &str) {
    let Some(path) = project_cache_path() else { return };
    let mut map = read_project_cache();
    if map.remove(name).is_some() {
        if let Ok(text) = serde_json::to_string_pretty(&serde_json::Value::Object(map)) {
            let _ = std::fs::write(path, text);
        }
    }
}

/// Describe whoever holds the lock, from the file's first line.
///
/// The contract with chatgpt-imagegen is one line, `<tool> <pid>`. Anything
/// else — empty file, a stale remnant on later lines, a tool that stamped only
/// its name, garbage — degrades to a usable phrase rather than failing: the
/// point is a legible waiter line, and the lock itself is what provides safety.
fn holder_label(contents: &str) -> String {
    let first = contents.lines().next().unwrap_or("").trim();
    if first.is_empty() {
        return "another chatgpt turn".to_string();
    }
    match first.split_once(char::is_whitespace) {
        Some((tool, rest)) => match rest.trim().parse::<u32>() {
            Ok(pid) => format!("{tool} (pid {pid})"),
            Err(_) => first.to_string(),
        },
        None => first.to_string(),
    }
}

/// `~/.chatgpt-web.lock` — deliberately NOT under `~/.chatgpt-use/`.
///
/// What this guards is the shared ChatGPT web surface, not one tool's state, and
/// more than one tool drives it (chatgpt-imagegen too). A lock living under one
/// project's directory invites the other project to pick its own path, and two
/// names mean no mutual exclusion at all — which is exactly the state that let
/// two prompts land in one composer.
fn lock_path() -> Option<PathBuf> {
    // Keep an explicit HOME for compatibility with other tools sharing this
    // lock, but also support native Windows shells where only USERPROFILE (or
    // HOMEDRIVE/HOMEPATH) is set. Missing HOME must not silently disable locking.
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(crate::platform::home_dir)
        .map(|home| home.join(".chatgpt-web.lock"))
}

#[cfg(test)]
#[path = "channel/lock_tests.rs"]
mod lock_tests;

/// Decide whether the tab has drifted off the pinned conversation.
///
/// Returns `None` when it's still the right chat, or `Some(explanation)` when the
/// turn must NOT proceed. Fail-closed by design: the multi-turn modes rely on
/// context accumulated in the pinned chat, so answering from a different one —
/// or from a blank new chat — is worse than erroring out.
fn convo_drift(pinned: &str, current: Option<&str>) -> Option<String> {
    match current {
        Some(cur) if cur == pinned => None,
        Some(cur) => Some(format!(
            "the browser tab moved to a different ChatGPT conversation \
             (expected {pinned}, found {cur}) — this channel's context lives in \
             the original chat. Leave the tab alone while it runs, or start a \
             new run."
        )),
        None => Some(format!(
            "the browser tab is no longer showing conversation {pinned} (it's on \
             a new/blank chat) — refusing to continue the turn there."
        )),
    }
}

/// What can actually be spawned, best first. Windows' CreateProcess cannot run
/// an extensionless file, so the `chrome-use` sh shim a layout puts on PATH must
/// never be chosen; a `.cmd` shim runs, but only through cmd.exe and its quoting,
/// so any real `.exe` beats it even when the shim comes earlier on PATH (#17).
#[cfg(windows)]
const BIN_SUFFIXES: &[&str] = &[".exe", ".cmd", ".bat"];
#[cfg(not(windows))]
const BIN_SUFFIXES: &[&str] = &[""];

fn find_chrome_use() -> Option<PathBuf> {
    // Prefer a companion binary next to chatgpt-use. This makes portable
    // Windows installs work even when the user's PATH is full or stale.
    let beside: Vec<PathBuf> = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(PathBuf::from))
        .into_iter()
        .collect();
    let path_var = std::env::var("PATH").unwrap_or_default();
    let on_path: Vec<PathBuf> = path_var
        .split(crate::platform::path_separator())
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
        .collect();
    // Common install spots that may not be on PATH.
    let mut usual: Vec<PathBuf> = Vec::new();
    if let Some(local) = std::env::var_os("LOCALAPPDATA") {
        usual.push(PathBuf::from(local).join("Programs").join("chrome-use"));
    }
    if let Some(home) = crate::platform::home_dir() {
        usual.push(home.join("chrome-tools"));
        usual.push(home.join(".local").join("bin"));
    }
    pick_binary(AB_BIN_CANDIDATES, &[beside, on_path, usual], BIN_SUFFIXES)
}

/// First existing `<dir>/<name><suffix>`. Suffix order outranks directory order,
/// so a better kind of binary anywhere wins over a worse one earlier in the search.
fn pick_binary(names: &[&str], tiers: &[Vec<PathBuf>], suffixes: &[&str]) -> Option<PathBuf> {
    for suffix in suffixes {
        for dirs in tiers {
            for name in names {
                for dir in dirs {
                    let candidate = dir.join(format!("{name}{suffix}"));
                    if candidate.is_file() {
                        return Some(candidate);
                    }
                }
            }
        }
    }
    None
}

/// The chrome-use argv for one upload. Pure, so its shape is testable without
/// a browser: `upload <selector> <files...>`.
fn upload_args(paths: &[PathBuf]) -> Result<Vec<String>> {
    let mut args = vec!["upload".to_string(), FILE_INPUT_SELECTOR.to_string()];
    for p in paths {
        let s = p
            .to_str()
            .with_context(|| format!("attachment path is not valid UTF-8: {}", p.display()))?;
        args.push(s.to_string());
    }
    Ok(args)
}

fn names(paths: &[PathBuf]) -> String {
    paths
        .iter()
        .map(|p| {
            p.file_name()
                .unwrap_or(p.as_os_str())
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Run a chrome-use subcommand with optional profile; return stdout.
/// Mirrors `_ab` in chatgpt-imagegen: `--profile <p>` precedes the subcommand;
/// `--session <s>` trails everything.
fn ab_cmd_with_profile(
    ab: &PathBuf,
    args: &[&str],
    session: &str,
    profile: Option<&str>,
    _timeout_secs: f64,
) -> Result<String> {
    let mut cmd = Command::new(ab);
    if let Some(prof) = profile {
        cmd.args(["--profile", prof]);
    }
    cmd.args(args);
    cmd.args(["--session", session]);

    let output = cmd
        .output()
        .map_err(|e| anyhow!("failed to run {} {args:?}: {e}", ab.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let tail: String = stderr
            .lines()
            .rev()
            .take(3)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join(" / ");
        let detail = if tail.is_empty() {
            stdout.trim().to_string()
        } else {
            tail
        };
        bail!(
            "chrome-use {:?} failed (exit {}): {}",
            args,
            output.status.code().unwrap_or(-1),
            crate::delegation::head(&detail, 300)
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Run a chrome-use subcommand (no profile override).
fn ab_cmd(ab: &PathBuf, args: &[&str], session: &str, timeout_secs: f64) -> Result<String> {
    ab_cmd_with_profile(ab, args, session, None, timeout_secs)
}

/// Run JS in the page and double-decode the returned JSON string.
///
/// Convention (mirrors `_ab_eval`): the JS does `return JSON.stringify(value)`;
/// chrome-use prints THAT string JSON-encoded, so we decode twice — once to get
/// the inner JSON text, once to parse it into a value.
fn ab_eval(
    ab: &PathBuf,
    js: &str,
    session: &str,
    timeout_secs: f64,
) -> Result<serde_json::Value> {
    let raw = ab_cmd(ab, &["eval", js], session, timeout_secs)?;

    // Scan from the last non-empty line for the first that decodes to a string.
    for line in raw.lines().rev() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // First decode: chrome-use wraps the page's return in a JSON string.
        if let Ok(inner) = serde_json::from_str::<serde_json::Value>(line) {
            if let Some(s) = inner.as_str() {
                // Second decode: the page did JSON.stringify(value).
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(s) {
                    return Ok(val);
                }
                // Not JSON-parseable — return as a plain string value.
                return Ok(serde_json::Value::String(s.to_string()));
            }
            // Not a string wrapper — return as-is.
            return Ok(inner);
        }
    }

    bail!(
        "could not parse chrome-use eval output: {:?}",
        &raw[..raw.len().min(200)]
    )
}

/// Open a URL in the session's tab (optionally with a Chrome profile).
fn ab_open(
    ab: &PathBuf,
    session: &str,
    url: &str,
    profile: Option<&str>,
    deadline: Instant,
) -> Result<()> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .unwrap_or(Duration::from_secs(2))
        .as_secs_f64()
        .min(30.0)
        .max(5.0);
    // A full navigation is the expensive request burst (about 45 for chatgpt.com).
    crate::throttle::note_event(crate::throttle::EVENT_PAGE_LOAD, serde_json::json!({ "url": url }));
    ab_cmd_with_profile(ab, &["open", url], session, profile, remaining)?;
    Ok(())
}

/// Close the session's tab — best effort, never raises.
fn ab_close(ab: &PathBuf, session: &str) {
    let _ = ab_cmd(ab, &["close"], session, 15.0);
}

/// Open a new chat tab and wait for the composer. Returns Ok(true) if ready.
fn try_open(
    ab: &PathBuf,
    session: &str,
    url: &str,
    profile: Option<&str>,
    deadline: Instant,
) -> Result<bool> {
    ab_open(ab, session, url, profile, deadline)?;
    wait_composer(ab, session, deadline, 15)
}

/// What the open page is, when the composer never appeared: whether it is
/// ChatGPT's signed-out screen (anything unreadable counts as "no": a login
/// claim needs positive evidence), and a short description for the error.
fn page_probe(ab: &PathBuf, session: &str) -> (bool, String) {
    match ab_eval(ab, JS_WANTS_LOGIN, session, 10.0) {
        Ok(v) => {
            let login = v.get("login").and_then(|b| b.as_bool()).unwrap_or(false);
            let field = |k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
            (login, format!("path {:?}, title {:?}", field("path"), field("title")))
        }
        Err(e) => (false, format!("page unreadable: {e}")),
    }
}

/// Poll until a composer is on the page (see `js_composer_helper!`).
/// Returns `Ok(true)` when the composer is ready, `Ok(false)` on timeout.
/// Bails with an error if the rate-limit dialog is detected.
fn wait_composer(
    ab: &PathBuf,
    session: &str,
    deadline: Instant,
    tries: u32,
) -> Result<bool> {
    for _ in 0..tries {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::from_secs(1))
            .as_secs_f64()
            .min(20.0)
            .max(2.0);

        match ab_eval(ab, JS_COMPOSER, session, remaining) {
            Ok(st) if st.is_object() => {
                if st.get("limited").and_then(|v| v.as_bool()).unwrap_or(false) {
                    return Err(rate_limited(RATE_LIMIT_MSG));
                }
                if st.get("composer").and_then(|v| v.as_bool()).unwrap_or(false) {
                    return Ok(true);
                }
            }
            _ => {}
        }

        if Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    Ok(false)
}

/// Detect Chrome profiles that have an active chatgpt.com session cookie.
/// Best-effort; returns an empty Vec rather than erroring (relay path still works).
/// The Python reference reads the Cookies SQLite DB; we skip that here to avoid
/// adding a sqlite3 dep — callers can pass --profile explicitly when needed.
fn detect_logged_in_profiles() -> Vec<String> {
    Vec::new()
}

// ---- tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    /// One test, not two: both halves have to move HOME, and cargo runs tests
    /// in parallel — as two tests they raced and the second read the first's
    /// directory.
    #[test]
    fn project_cache_round_trips_forgets_and_survives_corruption() {
        let tmp = std::env::temp_dir().join(format!("cgu-projcache-{}", std::process::id()));
        std::fs::create_dir_all(tmp.join(".chatgpt-use")).ok();
        let old_home = std::env::var_os("HOME");
        // SAFETY: the whole test body runs with HOME redirected and restores it.
        unsafe { std::env::set_var("HOME", &tmp) };

        assert_eq!(cached_gizmo("proj"), None);
        remember_gizmo("proj", "g-p-abc");
        assert_eq!(cached_gizmo("proj").as_deref(), Some("g-p-abc"));

        // A second project must not clobber the first.
        remember_gizmo("other", "g-p-def");
        assert_eq!(cached_gizmo("proj").as_deref(), Some("g-p-abc"));
        assert_eq!(cached_gizmo("other").as_deref(), Some("g-p-def"));

        // Forgetting a stale id must cost only that entry.
        forget_gizmo("proj");
        assert_eq!(cached_gizmo("proj"), None);
        assert_eq!(cached_gizmo("other").as_deref(), Some("g-p-def"));

        // A corrupt cache degrades to "no entry" and is rewritten, never panics.
        std::fs::write(tmp.join(".chatgpt-use").join("projects.json"), "{not json").ok();
        assert_eq!(cached_gizmo("other"), None);
        remember_gizmo("other", "g-p-def");
        assert_eq!(cached_gizmo("other").as_deref(), Some("g-p-def"));

        match old_home {
            Some(h) => unsafe { std::env::set_var("HOME", h) },
            None => unsafe { std::env::remove_var("HOME") },
        }
        std::fs::remove_dir_all(&tmp).ok();
    }

    /// The record carries markup the rendered page hides. Observed live in a
    /// `serve` reply: a citation span that would have been handed straight to
    /// Claude Code.
    #[test]
    fn strips_citation_spans_from_record_text() {
        let raw = "Tokyo is 21\u{b0}C. \u{e200}cite\u{e202}turn168423search4\u{e201}";
        assert_eq!(strip_private_markers(raw), "Tokyo is 21\u{b0}C.");
    }

    #[test]
    fn strips_bare_private_use_characters() {
        assert_eq!(strip_private_markers("a\u{e300}b"), "ab");
    }

    #[test]
    fn leaves_ordinary_text_alone() {
        let s = "**bold**, `code`, 中文, emoji \u{1F600}\n\nsecond paragraph";
        assert_eq!(strip_private_markers(s), s);
    }

    /// An unterminated span must not swallow the rest of the reply.
    #[test]
    fn an_unclosed_span_does_not_eat_the_tail() {
        // Depth stays open, so the tail is dropped — but a stray CLOSE marker
        // must never make depth negative and re-admit garbage.
        assert_eq!(strip_private_markers("keep\u{e201}more"), "keepmore");
    }

    /// These expected values were produced by running the EXACT JavaScript of
    /// `JS_COMPOSER_FINGERPRINT` under node. They are the contract between the
    /// two implementations: if Rust and the page ever disagree, every send
    /// fails the integrity check, so the agreement is pinned here rather than
    /// discovered in production.
    #[test]
    fn composer_fingerprint_matches_the_javascript() {
        assert_eq!(composer_fingerprint(""), (0, 2166136261));
        assert_eq!(composer_fingerprint("hello world"), (10, 942532069));
        assert_eq!(composer_fingerprint("AAA\nBBB\nCCC"), (9, 1755253517));
        assert_eq!(composer_fingerprint("中文测试 ABC"), (7, 1526566950));
        // Surrogate pair: four UTF-16 code units, not three chars.
        assert_eq!(composer_fingerprint("a\u{1F600}b"), (4, 957716613));
    }

    /// The three code points where `char::is_whitespace` and JavaScript's `\s`
    /// disagree must be classified the same by both sides. All three collapse
    /// to plain "ab".
    #[test]
    fn composer_fingerprint_agrees_on_contested_whitespace() {
        let plain = composer_fingerprint("ab");
        assert_eq!(plain, (2, 2174188438));
        assert_eq!(composer_fingerprint("a\u{00a0}b"), plain); // NBSP
        assert_eq!(composer_fingerprint("a\u{0085}b"), plain); // NEL — Rust-only in std
        assert_eq!(composer_fingerprint("a\u{feff}b"), plain); // BOM — JS-only in \s
    }

    /// The reason the hash exists at all: chrome-use#301 scrambles chunked
    /// inserts while PRESERVING length, so a count-only check waves it through.
    #[test]
    fn composer_fingerprint_detects_reordering_at_equal_length() {
        let (n1, h1) = composer_fingerprint("abcdef");
        let (n2, h2) = composer_fingerprint("abcdfe");
        assert_eq!(n1, n2, "the failure mode under test keeps the count identical");
        assert_ne!(h1, h2, "a reordered payload must not pass the integrity check");
        assert_eq!((n1, h1), (6, 829399410));
        assert_eq!((n2, h2), (6, 793662762));
    }

    #[test]
    fn holder_label_reads_tool_and_pid() {
        assert_eq!(holder_label("chatgpt-imagegen 4321\n"), "chatgpt-imagegen (pid 4321)");
    }

    /// The case that caught the O_APPEND bug on the imagegen side: a PREVIOUS
    /// holder's longer line left behind after ours. Ours is line 1; the remnant
    /// must be ignored, never reported as the holder.
    #[test]
    fn holder_label_ignores_a_longer_stale_remnant() {
        let f = "chatgpt-use 77\nchatgpt-imagegen 4321 some older longer line\n";
        assert_eq!(holder_label(f), "chatgpt-use (pid 77)");
    }

    #[test]
    fn holder_label_degrades_instead_of_failing() {
        assert_eq!(holder_label(""), "another chatgpt turn");
        assert_eq!(holder_label("   \n"), "another chatgpt turn");
        assert_eq!(holder_label("chatgpt-imagegen\n"), "chatgpt-imagegen");
        assert_eq!(holder_label("garbage not-a-pid\n"), "garbage not-a-pid");
    }

    #[test]
    fn level_index_maps_the_slider_positions() {
        assert_eq!(level_index("instant"), Some(0));
        assert_eq!(level_index("Pro"), Some(4));
        assert_eq!(level_index("extra high"), Some(3));
        assert_eq!(level_index("extra-high"), Some(3));
        assert_eq!(level_index("EXTRA   HIGH"), Some(3));
        assert_eq!(level_index("extrahigh"), Some(3));
    }

    /// A model family name must fall through to the radio path, not be
    /// mistaken for an effort level.
    #[test]
    fn level_index_rejects_model_family_names() {
        assert_eq!(level_index("GPT-5.5"), None);
        assert_eq!(level_index("Latest"), None);
        assert_eq!(level_index("5.6 Sol"), None);
    }

    #[test]
    fn picker_probe_is_structural_not_text_matched() {
        // The whole point: never key off the button label, which drifts.
        assert!(JS_FIND_PICKER.contains("composer-plus-btn"));
        assert!(JS_FIND_PICKER.contains(r#"button[aria-haspopup="menu"]"#));
        assert!(JS_FIND_PICKER.contains("model-switcher-dropdown-button"));
        assert!(JS_PICKER_MENU.contains("composer-intelligence-picker-content"));
        assert!(keeps_current_model("current") && keeps_current_model(" Default "));
        assert!(!keeps_current_model("instant") && !keeps_current_model("pro"));
        assert!(!JS_FIND_PICKER.to_lowercase().contains("instant"));
        assert!(JS_PICKER_MENU.contains(r#"[role="slider"]"#));
        assert!(JS_PICKER_MENU.contains("aria-valuenow"));
    }

    use super::*;

    #[test]
    fn convo_drift_allows_the_same_conversation() {
        assert!(convo_drift("abc", Some("abc")).is_none());
    }

    #[test]
    fn convo_drift_rejects_a_different_conversation() {
        let msg = convo_drift("abc", Some("xyz")).expect("must fail closed");
        assert!(msg.contains("abc") && msg.contains("xyz"), "{msg}");
    }

    #[test]
    fn upload_argv_names_the_stamped_input_and_every_file() {
        let paths = vec![
            PathBuf::from("/tmp/a.zip"),
            PathBuf::from("/tmp/b patch.patch"),
        ];
        let args = upload_args(&paths).unwrap();
        assert_eq!(
            args,
            vec![
                "upload",
                "[data-cgu-file-input]",
                "/tmp/a.zip",
                "/tmp/b patch.patch"
            ]
        );
    }

    #[test]
    fn the_file_input_probe_stamps_what_the_upload_targets() {
        assert!(JS_FILE_INPUT.contains("data-cgu-file-input"));
        assert!(JS_FILE_INPUT.contains(r#"input[type="file"]"#));
        assert!(JS_FILE_INPUT.contains("existing"));
        assert!(JS_ATTACHMENT_CHIPS.contains("chips"));
        assert!(ATTACHMENT_SELECTOR.contains("attachment"));
    }

    #[test]
    fn convo_drift_rejects_a_blank_new_chat() {
        let msg = convo_drift("abc", None).expect("must fail closed");
        assert!(msg.contains("abc"), "{msg}");
    }

    #[test]
    fn send_fallback_scopes_loose_selectors_to_the_composer_form() {
        for part in SEND_BUTTON_SELECTOR.split(", ") {
            let loose = part.contains("*=");
            assert!(
                !loose || part.starts_with("form:has([data-cgu-composer]) "),
                "loose selector not scoped to the composer form: {part}"
            );
        }
        assert!(SEND_BUTTON_SELECTOR.contains(r#"button[data-testid="send-button"]"#));
        assert!(SEND_BUTTON_SELECTOR.contains("composer-submit-button"));
        assert!(!SEND_BUTTON_SELECTOR.contains("stop"));
        assert!(JS_DISPATCH_ENTER.contains("[data-cgu-composer]"));
    }

    #[test]
    fn js_probes_target_the_selectors_we_depend_on() {
        assert!(JS_CONVO_ID.contains(r"/\/c\/([0-9a-f-]{36})/i"));
        assert!(JS_USER_TURNS.contains(r#"[data-message-author-role="${role}"]"#));
    }

    #[test]
    fn turn_probes_read_both_renderers() {
        // The newer renderer groups both roles under [data-turn-key] and may carry
        // no author-role attribute at all; every probe must still see its turns.
        // Behaviour is checked against DOM fixtures by scripts/check-dom.sh;
        // this only pins that every probe carries the shared helpers.
        for js in [JS_USER_TURNS, JS_STATE, JS_ASSISTANT_COUNT, JS_LAST_ASSISTANT] {
            assert!(js.contains("[data-user-message-bubble]"), "{js}");
            assert!(js.contains(r#"[data-conversation-role="assistant"]"#), "{js}");
            assert!(js.contains(r#"[data-content-search-unit-key$=":${role}"]"#), "{js}");
        }
        assert!(JS_USER_TURNS.contains("data-turn-id-container"));
        assert!(JS_CLICK_STOP.contains("composer-stop-button"));
        assert!(JS_STATE.contains("composer-stop-button"));
    }

    /// Writes the page probes for scripts/check-dom.sh. Ignored: it is a
    /// build step for that script, not a test.
    #[test]
    #[ignore]
    fn dump_probe_js() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/probe-js");
        std::fs::create_dir_all(&dir).unwrap();
        for (name, js) in [
            ("state", JS_STATE),
            ("user_turns", JS_USER_TURNS),
            ("assistant_count", JS_ASSISTANT_COUNT),
            ("last_assistant", JS_LAST_ASSISTANT),
            ("composer", JS_COMPOSER),
            ("dismiss_dialog", JS_DISMISS_DIALOG),
            ("click_stop", JS_CLICK_STOP),
            ("clear_composer", JS_CLEAR_COMPOSER),
            ("composer_fingerprint", JS_COMPOSER_FINGERPRINT),
            ("insert_hello", &js_insert_text("hello")),
            ("find_picker", JS_FIND_PICKER),
            ("mention_menu", &js_mention_menu("chatgpt-use")),
            ("connector_pill", &js_connector_pill("chatgpt-use")),
            ("picker_menu", JS_PICKER_MENU),
            ("server_final", &js_server_final("11111111-2222-3333-4444-555555555555")),
        ] {
            std::fs::write(dir.join(format!("{name}.js")), js).unwrap();
        }
    }

    #[test]
    fn rate_limit_probes_match_localized_dialogs() {
        for js in [JS_COMPOSER, JS_STATE, JS_DISMISS_DIALOG] {
            for phrase in ["too many requests", "太多请求", "リクエストが多すぎます", "요청이 너무 많습니다"] {
                assert!(js.contains(phrase), "{phrase} missing from {js}");
            }
        }
        // A bare unanchored "ok" would click any button whose label contains it.
        assert!(JS_DISMISS_DIALOG.contains("/^(got it|ok|"));
    }

    #[test]
    fn a_lost_tab_is_recognised_by_chrome_uses_wording() {
        assert!(tab_gone("chrome-use [\"open\"] failed (exit 1): ✗ the tab this command was driving is gone — it navigated"));
        assert!(!tab_gone("chrome-use failed: Too many requests"));
    }

    #[test]
    fn one_low_read_during_turn_one_is_not_a_lost_page() {
        let mut dips = 0;
        // The re-render blip: one low read, then the turn is back.
        assert!(!unpinned_page_lost(false, Some(0), 0, &mut dips));
        assert!(!unpinned_page_lost(false, Some(1), 0, &mut dips));
        assert_eq!(dips, 0);
        // A page that stays blank is lost on the third poll.
        assert!(!unpinned_page_lost(false, Some(0), 0, &mut dips));
        assert!(!unpinned_page_lost(false, Some(0), 0, &mut dips));
        assert!(unpinned_page_lost(false, Some(0), 0, &mut dips));
        // Once pinned, or with an unreadable count, the count never decides.
        let mut d = 2;
        assert!(!unpinned_page_lost(true, Some(0), 0, &mut d));
        assert_eq!(d, 0);
        assert!(!unpinned_page_lost(false, None, 0, &mut d));
    }

    #[test]
    fn the_fingerprint_check_fails_closed() {
        let ok = serde_json::json!({"n": 3, "h": 7});
        assert!(check_fingerprint(Some(&ok), 3, 7).is_ok());
        assert!(check_fingerprint(Some(&ok), 4, 7).unwrap_err().to_string().contains("truncated"));
        assert!(check_fingerprint(Some(&ok), 3, 8).unwrap_err().to_string().contains("scrambled"));
        // A probe that failed or answered in another shape is not a pass.
        for bad in [None, Some(serde_json::json!({})), Some(serde_json::json!({"n": 3})),
                    Some(serde_json::json!("error"))] {
            let e = check_fingerprint(bad.as_ref(), 3, 7).unwrap_err().to_string();
            assert!(e.contains("not verified"), "{e}");
        }
    }

    #[test]
    fn a_task_cancel_flag_is_scoped_to_its_thread_and_call() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        let flag = Arc::new(AtomicBool::new(false));
        assert!(!cancel_requested());
        with_cancel_flag(flag.clone(), || {
            assert!(!cancel_requested());
            flag.store(true, Ordering::SeqCst);
            assert!(cancel_requested());
            // Another thread, another task: not cancelled.
            assert!(!std::thread::spawn(cancel_requested).join().unwrap());
        });
        // The next task on this thread starts clean.
        assert!(!cancel_requested());
    }

    fn turns(count: u64, ids: &[&str], known: &[&str]) -> UserTurns {
        UserTurns {
            count,
            ids: ids.iter().map(|s| s.to_string()).collect(),
            known: known.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn user_turn_rose_on_a_fresh_chat() {
        assert!(user_turn_rose(&turns(0, &[], &[]), &turns(1, &["u1"], &["u1"])));
        assert!(!user_turn_rose(&turns(0, &[], &[]), &turns(0, &[], &[])));
    }

    #[test]
    fn user_turn_rose_by_identity_despite_virtualization() {
        // Two old turns at baseline; one scrolls out as ours arrives: count flat.
        let base = turns(2, &["u1", "u2"], &["u1", "u2"]);
        assert!(user_turn_rose(&base, &turns(2, &["u2", "u3"], &["u1", "u2", "u3"])));
    }

    #[test]
    fn remounting_an_old_turn_is_not_a_submit() {
        // u1 was virtualized at baseline (container only); it remounts later.
        let base = turns(1, &["u2"], &["u1", "u2"]);
        assert!(!user_turn_rose(&base, &turns(2, &["u1", "u2"], &["u1", "u2"])));
    }

    #[test]
    fn grouped_renderer_identities_work_too() {
        let base = turns(1, &["group:a"], &["group:a"]);
        assert!(user_turn_rose(&base, &turns(2, &["group:a", "group:b"], &["group:a", "group:b"])));
    }

    #[test]
    fn user_turn_rose_falls_back_to_count_without_identities() {
        assert!(user_turn_rose(&turns(2, &[], &[]), &turns(3, &[], &[])));
        assert!(!user_turn_rose(&turns(2, &[], &[]), &turns(2, &[], &[])));
        // One rendered turn has no identity: the id test could miss it, so count.
        assert!(user_turn_rose(&turns(1, &["u1"], &["u1"]), &turns(2, &["u1"], &["u1"])));
    }

    #[test]
    fn user_turns_parse_from_probe_json() {
        let v = serde_json::json!({"count": 2, "ids": ["a", "b"], "known": ["a", "b", "c"]});
        assert_eq!(UserTurns::from_json(&v), Some(turns(2, &["a", "b"], &["a", "b", "c"])));
        assert_eq!(UserTurns::from_json(&serde_json::json!(3)), None);
    }

    #[test]
    fn js_ensure_project_embeds_name() {
        let js = js_ensure_project("my-project");
        assert!(js.contains("my-project"), "JS should embed the project name");
        assert!(js.contains("backend-api/projects"), "JS should reference the project API");
    }

    #[test]
    fn find_chrome_use_returns_option() {
        // Verify the function runs without panic; result depends on the host.
        let _ = find_chrome_use();
    }

    #[test]
    fn ab_eval_double_decode_logic() {
        // Simulate chrome-use output: the page returned JSON.stringify({key:"val"}),
        // so chrome-use printed the JSON-encoded wrapper: "\"{\\\"key\\\":\\\"val\\\"}\"".
        // The decode logic should produce Value::Object({key: "val"}).
        let page_value = serde_json::json!({"key": "val"});
        let page_json = serde_json::to_string(&page_value).unwrap(); // {"key":"val"}
        let chrome_use_line = serde_json::to_string(&page_json).unwrap(); // "\"{...}\""

        // Reproduce the decode loop from ab_eval.
        let inner: serde_json::Value = serde_json::from_str(&chrome_use_line).unwrap();
        assert!(inner.is_string());
        let second: serde_json::Value =
            serde_json::from_str(inner.as_str().unwrap()).unwrap();
        assert_eq!(second["key"], "val");
    }

    #[test]
    fn a_windows_lookup_skips_the_extensionless_shim_for_a_real_exe() {
        // #17: chrome-tools/bin comes first on PATH and holds a sh shim, a .cmd
        // shim and no .exe; the real binary sits in a later PATH directory.
        let root = std::env::temp_dir().join(format!("cgu-pick-{}", std::process::id()));
        let (shims, real) = (root.join("bin"), root.join("Programs"));
        std::fs::create_dir_all(&shims).unwrap();
        std::fs::create_dir_all(&real).unwrap();
        std::fs::write(shims.join("chrome-use"), "#!/bin/sh").unwrap();
        let tiers = [vec![shims.clone(), real.clone()]];
        let windows = [".exe", ".cmd", ".bat"];

        // The sh shim alone is not runnable: nothing to pick.
        assert_eq!(pick_binary(&["chrome-use"], &tiers, &windows), None);

        std::fs::write(shims.join("chrome-use.cmd"), "@echo off").unwrap();
        assert_eq!(pick_binary(&["chrome-use"], &tiers, &windows), Some(shims.join("chrome-use.cmd")));

        std::fs::write(real.join("chrome-use.exe"), "MZ").unwrap();
        assert_eq!(pick_binary(&["chrome-use"], &tiers, &windows), Some(real.join("chrome-use.exe")));

        // Elsewhere the plain name is the binary, and earlier tiers still win.
        assert_eq!(pick_binary(&["chrome-use"], &tiers, &[""]), Some(shims.join("chrome-use")));
        let _ = std::fs::remove_dir_all(&root);
    }

    fn typed(kind: ErrorKind) -> anyhow::Error {
        ChannelError::new(kind, "boom").into()
    }

    #[test]
    fn channel_error_survives_context_layers() {
        let e = typed(ErrorKind::RateLimited).context("resubmitting").context("outer");
        assert_eq!(channel_error(&e).map(|c| c.kind), Some(ErrorKind::RateLimited));
    }

    #[test]
    fn untyped_failures_are_typed_by_how_far_the_turn_got() {
        let before = classify(anyhow!("tab vanished"), false);
        let ce = channel_error(&before).unwrap();
        assert_eq!((ce.kind, ce.submitted), (ErrorKind::NotSubmitted, Submitted::No));
        assert!(format!("{before:#}").contains("tab vanished"), "the cause is kept");

        let after = classify(anyhow!("page swapped"), true);
        let ce = channel_error(&after).unwrap();
        assert_eq!((ce.kind, ce.submitted), (ErrorKind::Incomplete, Submitted::Yes));
    }

    #[test]
    fn a_typed_failure_keeps_its_kind_and_takes_the_turn_phase() {
        let e = classify(typed(ErrorKind::RateLimited).context("while polling"), true);
        let ce = channel_error(&e).unwrap();
        assert_eq!((ce.kind, ce.submitted), (ErrorKind::RateLimited, Submitted::Yes));
    }

    #[test]
    fn nothing_downgrades_an_ambiguous_submit() {
        for phase in [false, true] {
            let e = SubmitFailure::Ambiguous(anyhow!("no receipt")).into_error();
            let e = classify(e.context("x"), phase);
            let ce = channel_error(&e).map(|c| (c.kind, c.submitted));
            assert_eq!(ce, Some((ErrorKind::SubmitUnknown, Submitted::Unknown)));
        }
        // Also when the ambiguity was itself caused by a typed failure.
        let e = SubmitFailure::Ambiguous(typed(ErrorKind::RateLimited)).into_error();
        let e = classify(e, false);
        let ce = channel_error(&e).unwrap();
        assert_eq!((ce.kind, ce.submitted), (ErrorKind::RateLimited, Submitted::Unknown));
    }

    #[test]
    fn no_failure_maps_to_completed() {
        use ErrorKind::*;
        for k in [
            LoginRequired, RateLimited, SessionUnavailable, PageBlocked, NotSubmitted, SubmitUnknown,
            Incomplete, Busy, Duplicate, Cancelled, CancelRequested,
        ] {
            assert_ne!(k.status(), "completed", "{k:?}");
        }
        assert_eq!(Incomplete.status(), "incomplete");
        assert_eq!(RateLimited.status(), "unavailable");
    }

    /// A record projection as `js_server_final` emits it.
    fn record(nodes: serde_json::Value, user: &str) -> RecordTurn {
        RecordTurn::from_json(&serde_json::json!({
            "conversation_id": "c1",
            "current_node": "n",
            "user_text": user,
            "nodes": nodes,
        }))
        .expect("a projection with nodes parses")
    }

    /// An assistant node with the defaults a finished answer carries.
    fn assistant(id: &str, text: &str, extra: serde_json::Value) -> serde_json::Value {
        let mut n = serde_json::json!({
            "id": id, "role": "assistant", "content_type": "text",
            "channel": "final", "recipient": "all", "weight": 1,
            "end_turn": true, "status": "finished_successfully",
            "is_complete": true, "hidden": false, "finish": "", "reason": "",
            "text": text,
        });
        for (k, v) in extra.as_object().expect("object") {
            n[k] = v.clone();
        }
        n
    }

    /// The measured defect: the newest node is a visible progress message, and
    /// the old probe returned it as a finished answer.
    #[test]
    fn an_intermediate_step_is_not_the_answer() {
        let t = record(
            serde_json::json!([
                assistant("a2", "Now running the tests.", serde_json::json!({"channel": "commentary"})),
                assistant("a1", "I'll inspect the repo first.", serde_json::json!({
                    "channel": "commentary", "end_turn": false,
                })),
            ]),
            "do the thing",
        );
        assert_eq!(judge_record(&t), Verdict::Open);
    }

    /// A record that marks no channel cannot tell a step from the answer: a
    /// multi-step turn closes each step with `end_turn` too. The same shape
    /// that closed a turn early must stay open here.
    #[test]
    fn a_record_without_channels_cannot_prove_the_turn_closed() {
        let step = record(
            serde_json::json!([assistant("a2", "Step 2: running tests", serde_json::json!({
                "channel": "",
            }))]),
            "q",
        );
        assert_eq!(judge_record(&step), Verdict::Open);

        // A channel-marked neighbour does not license reading a channel-less
        // message as the answer either; only `final` is the answer.
        let mixed = record(
            serde_json::json!([
                assistant("a2", "the answer?", serde_json::json!({"channel": ""})),
                {"id":"t1","role":"tool","content_type":"tool_result",
                 "channel":"commentary","status":"finished_successfully","text":""},
            ]),
            "q",
        );
        assert_eq!(judge_record(&mixed), Verdict::Open);
    }

    /// An older node still marked `in_progress` cannot be written once a newer
    /// answer closed, so it must not hold the finished turn open.
    #[test]
    fn a_stale_step_below_the_answer_does_not_hold_the_turn_open() {
        let t = record(
            serde_json::json!([
                assistant("a1", "the answer", serde_json::json!({"finish": "stop"})),
                {"id":"t1","role":"assistant","content_type":"tool_result",
                 "channel":"commentary","status":"in_progress",
                 "node_status":"in_progress","text":""},
            ]),
            "q",
        );
        assert!(matches!(judge_record(&t), Verdict::Done(a) if a.text == "the answer"));
    }

    /// A record that does not name its user turn could be the previous turn,
    /// whose answer is already closed. It is not proof of this turn.
    #[test]
    fn a_record_that_does_not_name_the_turn_is_not_ours() {
        let t = RecordTurn::from_json(&serde_json::json!({
            "conversation_id": "c1",
            "nodes": [],
        }))
        .expect("parses");
        assert!(!record_is_ours(&t, Some("c1"), Some("q"), false));

        // With no prompt to compare (resume), there is nothing to contradict.
        assert!(record_is_ours(&t, Some("c1"), None, false));
    }

    /// A thinking step is newer than the visible message below it, so that
    /// message is not final either.
    #[test]
    fn a_thinking_step_above_the_answer_keeps_the_turn_open() {
        let t = record(
            serde_json::json!([
                {"id":"t1","role":"assistant","content_type":"thoughts",
                 "status":"finished_successfully","weight":1,"text":""},
                assistant("a1", "an older visible message", serde_json::json!({})),
            ]),
            "q",
        );
        assert_eq!(judge_record(&t), Verdict::Open);
    }

    /// The one thing that may follow the answer: ChatGPT's own "Thought for Xs"
    /// disclosure.
    #[test]
    fn the_reasoning_recap_after_the_answer_is_not_a_new_step() {
        let t = record(
            serde_json::json!([
                {"id":"r1","role":"assistant","content_type":"reasoning_recap",
                 "status":"finished_successfully","weight":1,"text":"Thought for 5s"},
                assistant("a1", "the answer", serde_json::json!({})),
            ]),
            "q",
        );
        assert!(matches!(judge_record(&t), Verdict::Done(a) if a.text == "the answer"));
    }

    #[test]
    fn a_turn_that_is_still_streaming_is_open() {
        let t = record(
            serde_json::json!([assistant("a1", "half a sen", serde_json::json!({
                "end_turn": false, "status": "in_progress",
            }))]),
            "q",
        );
        assert_eq!(judge_record(&t), Verdict::Open);
    }

    #[test]
    fn a_real_finish_is_a_reply_and_an_unknown_finish_still_passes() {
        let done = record(serde_json::json!([assistant("a1", "done", serde_json::json!({
            "finish": "stop",
        }))]), "q");
        assert!(matches!(judge_record(&done), Verdict::Done(a) if a.text == "done"));

        // An unknown finish type is not treated as a failure: a model that
        // labels its stop differently must keep working.
        let odd = record(serde_json::json!([assistant("a1", "done", serde_json::json!({
            "finish": "new_kind",
        }))]), "q");
        assert!(matches!(judge_record(&odd), Verdict::Done(_)));
    }

    /// Short is not a placeholder. The old code's only check was non-empty.
    #[test]
    fn a_short_answer_is_a_reply() {
        let t = record(serde_json::json!([assistant("a1", "OK", serde_json::json!({}))]), "q");
        assert!(matches!(judge_record(&t), Verdict::Done(a) if a.text == "OK"));
    }

    #[test]
    fn a_stopped_turn_is_incomplete_not_a_reply() {
        // Exactly what the record showed after the stop button, live.
        let t = record(serde_json::json!([assistant("a1", "partial essay", serde_json::json!({
            "finish": "interrupted", "reason": "client_stopped",
        }))]), "q");
        match judge_record(&t) {
            Verdict::CutOff { kind, reason, chars } => {
                assert_eq!(kind, CutOffKind::Interrupted);
                assert_eq!(reason.as_deref(), Some("client_stopped"));
                assert_eq!(chars, "partial essay".chars().count());
                let e = kind.error(reason, chars);
                let ce = channel_error(&e).unwrap();
                assert_eq!((ce.kind, ce.submitted), (ErrorKind::Incomplete, Submitted::Yes));
                assert!(e.to_string().contains("client_stopped"), "{e}");
            }
            v => panic!("{v:?}"),
        }
    }

    #[test]
    fn a_length_capped_turn_is_incomplete_even_with_no_text() {
        let t = record(serde_json::json!([assistant("a1", "", serde_json::json!({
            "finish": "max_tokens",
        }))]), "q");
        match judge_record(&t) {
            Verdict::CutOff { kind, chars, .. } => {
                assert_eq!(kind, CutOffKind::MaxTokens);
                assert_eq!(chars, 0);
            }
            v => panic!("{v:?}"),
        }
    }

    /// A thinking step cut off before a finished answer is not this turn's end.
    #[test]
    fn a_cut_off_step_before_the_answer_is_not_the_ending() {
        let t = record(
            serde_json::json!([
                assistant("a1", "the answer", serde_json::json!({"finish": "stop"})),
                {"id":"t1","role":"assistant","content_type":"thoughts",
                 "status":"finished_successfully","weight":1,
                 "finish":"interrupted","text":""},
            ]),
            "q",
        );
        assert!(matches!(judge_record(&t), Verdict::Done(a) if a.text == "the answer"));
    }

    /// The exact projections the page probe returned for the three fixtures
    /// (measured offline under node), fed through the Rust judge. Before the
    /// split, the probe answered `done: true` with the intermediate text for
    /// the first two.
    #[test]
    fn the_measured_progress_projections_are_not_answers() {
        let in_progress = serde_json::json!({
            "ok": true, "conversation_id": "c1", "current_node": "a2", "async_status": 4,
            "user_text": "u1 text",
            "nodes": [
                {"id":"a2","role":"assistant","content_type":"text","channel":"commentary",
                 "recipient":"","weight":1,"end_turn":true,"status":"finished_successfully",
                 "is_complete":true,"hidden":false,"finish":"","reason":"","text":"a2 text"},
                {"id":"a1","role":"assistant","content_type":"text","channel":"commentary",
                 "recipient":"","weight":1,"end_turn":true,"status":"finished_successfully",
                 "is_complete":true,"hidden":false,"finish":"","reason":"","text":"a1 text"}
            ]
        });
        let thinking = serde_json::json!({
            "ok": true, "conversation_id": "c1", "current_node": "t1", "async_status": 4,
            "user_text": "u1 text",
            "nodes": [
                {"id":"t1","role":"assistant","content_type":"thoughts","channel":"",
                 "recipient":"","weight":1,"end_turn":false,"status":"in_progress",
                 "is_complete":true,"hidden":false,"finish":"","reason":"","text":""},
                {"id":"a1","role":"assistant","content_type":"text","channel":"commentary",
                 "recipient":"","weight":1,"end_turn":true,"status":"finished_successfully",
                 "is_complete":true,"hidden":false,"finish":"","reason":"","text":"a1 text"}
            ]
        });
        let finished = serde_json::json!({
            "ok": true, "conversation_id": "c1", "current_node": "a2", "async_status": null,
            "user_text": "u1 text",
            "nodes": [
                {"id":"a2","role":"assistant","content_type":"text","channel":"final",
                 "recipient":"","weight":1,"end_turn":true,"status":"finished_successfully",
                 "is_complete":true,"hidden":false,"finish":"stop","reason":"","text":"a2 text"},
                {"id":"a1","role":"assistant","content_type":"text","channel":"commentary",
                 "recipient":"web.run","weight":1,"end_turn":false,"status":"finished_successfully",
                 "is_complete":true,"hidden":false,"finish":"","reason":"","text":"a1 text"}
            ]
        });
        let interrupted = serde_json::json!({
            "ok": true, "conversation_id": "c1", "user_text": "u1 text",
            "nodes": [
                {"id":"a1","role":"assistant","content_type":"text","channel":"final",
                 "recipient":"","weight":1,"end_turn":true,"status":"finished_successfully",
                 "node_status":"","is_complete":true,"hidden":false,
                 "finish":"interrupted","reason":"client_stopped","text":"a1 text"}
            ]
        });
        let max_tokens = serde_json::json!({
            "ok": true, "conversation_id": "c1", "user_text": "u1 text",
            "nodes": [
                {"id":"a1","role":"assistant","content_type":"text","channel":"final",
                 "recipient":"","weight":1,"end_turn":true,"status":"finished_successfully",
                 "node_status":"","is_complete":true,"hidden":false,
                 "finish":"max_tokens","reason":"","text":"a1 text"}
            ]
        });
        let multi_final = serde_json::json!({
            "ok": true, "conversation_id": "c1", "user_text": "u1 text",
            "nodes": [
                {"id":"a2","role":"assistant","content_type":"text","channel":"final",
                 "recipient":"","weight":1,"end_turn":true,"status":"finished_successfully",
                 "node_status":"","is_complete":true,"hidden":false,"finish":"","reason":"",
                 "text":"a2 text"},
                {"id":"a1","role":"assistant","content_type":"text","channel":"final",
                 "recipient":"","weight":1,"end_turn":true,"status":"finished_successfully",
                 "node_status":"","is_complete":true,"hidden":false,"finish":"","reason":"",
                 "text":"a1 text"}
            ]
        });
        for (name, v) in [("in-progress", in_progress), ("thinking", thinking)] {
            let t = RecordTurn::from_json(&v).expect("parses");
            assert_eq!(judge_record(&t), Verdict::Open, "{name}");
        }
        let t = RecordTurn::from_json(&finished).expect("parses");
        assert!(matches!(judge_record(&t), Verdict::Done(a) if a.text == "a2 text"));

        let t = RecordTurn::from_json(&interrupted).expect("parses");
        match judge_record(&t) {
            Verdict::CutOff { kind, reason, .. } => {
                assert_eq!(kind, CutOffKind::Interrupted);
                assert_eq!(reason.as_deref(), Some("client_stopped"));
            }
            v => panic!("{v:?}"),
        }
        let t = RecordTurn::from_json(&max_tokens).expect("parses");
        assert!(matches!(
            judge_record(&t),
            Verdict::CutOff { kind: CutOffKind::MaxTokens, .. }
        ));
        let t = RecordTurn::from_json(&multi_final).expect("parses");
        assert!(matches!(judge_record(&t), Verdict::Done(a) if a.text == "a2 text"));
    }

    /// Measured live on 2026-11-16: the record of a web-search turn, newest
    /// first. A tool call and two thinking nodes sit below the final answer,
    /// so a judge that reads any step as the ending would answer with an
    /// intermediate message or nothing at all.
    #[test]
    fn the_measured_search_turn_closes_on_the_final_answer() {
        let t = record(
            serde_json::json!([
                assistant(
                    "f1",
                    "RUST=1.100.0 (blog.rust-lang.org)\nNODE=26.13.1 (nodejs.org)",
                    serde_json::json!({"finish": "stop"}),
                ),
                {"id":"r1","role":"assistant","content_type":"reasoning_recap",
                 "channel":"","status":"finished_successfully","weight":1,
                 "end_turn":false,"text":""},
                {"id":"t1","role":"assistant","content_type":"thoughts",
                 "channel":"","status":"finished_successfully","weight":1,
                 "end_turn":false,"text":""},
                {"id":"tool1","role":"tool","content_type":"text","channel":"",
                 "status":"finished_successfully","text":""},
                {"id":"step1","role":"assistant","content_type":"text","channel":"",
                 "end_turn":false,"finish":"stop","status":"finished_successfully",
                 "weight":1,"text":""},
            ]),
            "q",
        );
        assert!(matches!(
            judge_record(&t),
            Verdict::Done(a) if a.text.starts_with("RUST=1.100.0")
        ));
    }

    /// Measured live on 2026-11-16: a long answer was regenerated, and the
    /// record kept the regenerated tail as a hidden node ABOVE the visible
    /// complete answer. The hidden node is not rendered, so it can neither be
    /// the reply nor newer writing that holds the turn open. Its interrupted
    /// marker must not be read as this turn's ending either.
    #[test]
    fn a_hidden_regenerated_tail_is_not_part_of_the_conversation() {
        let t = record(
            serde_json::json!([
                assistant("h1", "## 5. 확장성\n\nthe regenerated tail", serde_json::json!({
                    "hidden": true, "finish": "interrupted",
                })),
                assistant("a1", "## 1. 결론\n\nthe visible complete answer", serde_json::json!({})),
            ]),
            "q",
        );
        assert!(matches!(
            judge_record(&t),
            Verdict::Done(a) if a.text == "## 1. 결론\n\nthe visible complete answer"
        ));
    }

    /// A regenerated or superseded final message is not part of the reply. The
    /// newest one is. Joining them would also glue an artifact to the text and
    /// break a ```json fence the schema validator needs.
    #[test]
    fn only_the_newest_final_message_is_the_reply() {
        let t = record(
            serde_json::json!([
                assistant("a2", "the newer answer", serde_json::json!({})),
                assistant("a1", "the older answer", serde_json::json!({})),
            ]),
            "q",
        );
        assert!(matches!(judge_record(&t), Verdict::Done(a) if a.text == "the newer answer"));
    }

    #[test]
    fn an_empty_final_message_is_not_a_reply() {
        let t = record(
            serde_json::json!([assistant("a1", "   ", serde_json::json!({}))]),
            "q",
        );
        assert_eq!(judge_record(&t), Verdict::Open);
    }

    /// A failed read or a payload without the projection is "no proof", never
    /// an answer and never a hard failure.
    #[test]
    fn an_unreadable_record_is_no_proof() {
        assert!(RecordTurn::from_json(&serde_json::json!({
            "ok": false, "error": "HTTP 500",
        }))
        .is_none());
        assert!(RecordTurn::from_json(&serde_json::json!({"ok": true})).is_none());
    }

    /// The guard as a table: none of these shapes may be reported as a
    /// finished reply, whatever their own flags say. This is the invariant the
    /// bug broke, so it is asserted once over every non-final shape we know.
    #[test]
    fn no_non_final_shape_is_ever_done() {
        let shapes: Vec<(&str, serde_json::Value)> = vec![
            (
                "commentary leaf",
                serde_json::json!([assistant("a1", "step", serde_json::json!({
                    "channel": "commentary",
                }))]),
            ),
            (
                "analysis leaf",
                serde_json::json!([assistant("a1", "analysis", serde_json::json!({
                    "channel": "analysis",
                }))]),
            ),
            (
                "thoughts above the answer",
                serde_json::json!([
                    {"id":"t","role":"assistant","content_type":"thoughts",
                     "status":"finished_successfully","weight":1,"text":""},
                    assistant("a1", "answer", serde_json::json!({})),
                ]),
            ),
            (
                "message in progress",
                serde_json::json!([assistant("a1", "half", serde_json::json!({
                    "status": "in_progress",
                }))]),
            ),
            (
                "node in progress",
                serde_json::json!([assistant("a1", "half", serde_json::json!({
                    "node_status": "in_progress",
                }))]),
            ),
            (
                "end_turn false",
                serde_json::json!([assistant("a1", "half", serde_json::json!({
                    "end_turn": false,
                }))]),
            ),
            (
                "not complete",
                serde_json::json!([assistant("a1", "half", serde_json::json!({
                    "is_complete": false,
                }))]),
            ),
            (
                "failed status",
                serde_json::json!([assistant("a1", "answer", serde_json::json!({
                    "status": "failed",
                }))]),
            ),
            (
                "empty text",
                serde_json::json!([assistant("a1", "   ", serde_json::json!({}))]),
            ),
            (
                "hidden answer",
                serde_json::json!([assistant("a1", "hidden", serde_json::json!({
                    "hidden": true,
                }))]),
            ),
            (
                "weight zero",
                serde_json::json!([assistant("a1", "weightless", serde_json::json!({
                    "weight": 0,
                }))]),
            ),
            (
                "tool recipient",
                serde_json::json!([assistant("a1", "tool output", serde_json::json!({
                    "recipient": "web.run",
                }))]),
            ),
            ("no nodes at all", serde_json::json!([])),
        ];
        for (name, nodes) in shapes {
            let t = record(nodes, "q");
            assert_eq!(judge_record(&t), Verdict::Open, "{name}");
        }
    }

    /// The exact string that reached a `completed` envelope.
    #[test]
    fn the_chrome_header_alone_is_not_a_reply() {
        assert_eq!(usable_reply("ChatGPT 답변:ChatGPT", ""), None);
        assert_eq!(usable_reply("ChatGPT said:", ""), None);
        assert_eq!(usable_reply("   ", ""), None);
        assert_eq!(
            usable_reply("ChatGPT 답변: 실제 답변", ""),
            Some("실제 답변".into())
        );
        // Prose that starts with a label word is not a header.
        assert_eq!(
            usable_reply("You can do this.", ""),
            Some("You can do this.".into())
        );
        assert_eq!(usable_reply("OK", ""), Some("OK".into()));
    }

    #[test]
    fn the_tab_title_is_not_a_reply() {
        assert_eq!(usable_reply("Fix the parser", "Fix the parser"), None);
    }

    #[test]
    fn the_record_must_describe_this_turn() {
        let t = record(serde_json::json!([assistant("a1", "done", serde_json::json!({}))]), "our prompt");
        assert!(record_is_ours(&t, Some("c1"), Some("our prompt"), false));
        assert!(!record_is_ours(&t, Some("c2"), Some("our prompt"), false), "wrong conversation");
        assert!(!record_is_ours(&t, Some("c1"), Some("another prompt"), false), "wrong prompt");

        // A connector pill is prepended to the submitted text.
        let with_pill = record(
            serde_json::json!([assistant("a1", "done", serde_json::json!({}))]),
            "@app our prompt",
        );
        assert!(record_is_ours(&with_pill, Some("c1"), Some("our prompt"), true));
        // Resume attaches without a prompt; the id is the only identity it has.
        assert!(record_is_ours(&t, Some("c1"), None, false));
    }

    #[test]
    fn a_long_prompt_is_identified_by_its_opening() {
        let long = format!("{} tail", "a".repeat(300));
        assert!(same_prompt(&long, &format!("{} tail truncated", "a".repeat(300)), false));
        assert!(!same_prompt(&long, &format!("{} tail", "b".repeat(300)), false));
    }

    #[test]
    fn a_settled_page_asks_the_record_and_never_answers_itself() {
        let now = Instant::now();
        let mut w = Watch::new(&SendOptions::default(), now);
        let dom = DomProgress {
            stop: false,
            tool_active: false,
            new_turn: true,
            text: "step one",
        };

        // First poll: the record is asked, and the page is not yet settled.
        assert!(w.observe(dom, now));
        assert!(!w.settled());
        w.record_open(now);
        // The gate holds the next ask back; the same text now counts as stable.
        assert!(!w.observe(dom, now + Duration::from_secs(1)));
        assert!(!w.observe(dom, now + Duration::from_secs(2)));
        assert!(w.settled(), "two unchanged polls with no stop button");
        // A readable "still open" answer spends the settle evidence.
        assert!(w.observe(dom, now + Duration::from_secs(21)));
        w.record_open(now + Duration::from_secs(21));
        assert!(!w.settled());
    }

    #[test]
    fn a_streaming_page_resets_the_settle_evidence() {
        let now = Instant::now();
        let mut w = Watch::new(&SendOptions::default(), now);
        let still = DomProgress {
            stop: false,
            tool_active: false,
            new_turn: true,
            text: "same",
        };
        w.observe(still, now);
        w.observe(still, now + Duration::from_secs(2));
        w.observe(still, now + Duration::from_secs(4));
        assert!(w.settled());
        // The stop button reappears: generation resumed, so the evidence is gone.
        w.observe(
            DomProgress { stop: true, ..still },
            now + Duration::from_secs(6),
        );
        assert!(!w.settled());
    }

    #[test]
    fn an_unreadable_record_backs_off_to_a_ceiling() {
        let now = Instant::now();
        let mut w = Watch::new(&SendOptions::default(), now);
        let mut at = now;
        let mut waits = Vec::new();
        for _ in 0..6 {
            w.record_unreadable(at);
            waits.push((w.next_record - at).as_secs());
            at = w.next_record;
        }
        assert_eq!(waits, vec![2, 4, 8, 16, 30, 30]);
    }

    #[test]
    fn the_record_probe_projects_what_the_judge_needs() {
        let js = js_server_final("c-1");
        for field in [
            "channel",
            "end_turn",
            "status",
            "node_status",
            "is_complete",
            "recipient",
            "user_text",
            "finish_details",
        ] {
            assert!(js.contains(field), "{field} missing from the probe");
        }
        assert!(
            js.contains("m.author.role === 'user')"),
            "must not reach the previous turn"
        );
    }

    #[test]
    fn the_page_state_carries_the_title_for_placeholder_checks() {
        assert!(JS_STATE.contains("title: (document.title"), "{JS_STATE}");
    }

    #[test]
    fn opening_a_project_in_place_only_clicks_the_project_page_link() {
        let js = js_open_project_in_place("g-p-abc");
        assert!(js.contains(r"/\/project\/?$/"), "{js}");
    }
}
