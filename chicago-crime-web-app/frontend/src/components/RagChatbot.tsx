import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { Menu, Send } from "lucide-react";
import LimitationsModal from "@/components/LimitationsModal";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { cn } from "@/lib/utils";
import {
  Table,
  TableBody,
  TableCell,
  TableFooter,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

type Citation = {
  source?: string;
  title?: string | null;
  url?: string | null;
  detail?: string | null;
};

type StoredMessage = {
  role: "user" | "assistant";
  content: string;
  citations?: Citation[];
  thinking?: string[];
  steps?: string[];
};

type ChatSession = {
  id: string;
  title: string;
  messages: StoredMessage[];
};

const STORAGE_KEY = "ccas_sessions";

function newId() {
  // crypto.randomUUID is supported in modern browsers.
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) return crypto.randomUUID();
  return String(Date.now() + Math.random());
}

function loadSessions(): ChatSession[] {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .filter((s) => s && typeof s.id === "string")
      .map((s) => ({
        id: s.id,
        title: typeof s.title === "string" ? s.title : "",
        messages: Array.isArray(s.messages) ? s.messages : [],
      }));
  } catch {
    return [];
  }
}

function truncateTitle(question: string) {
  if (question.length <= 48) return question;
  return `${question.slice(0, 48)}…`;
}

function CitationsToggle({ citations }: { citations: Citation[] }) {
  const [open, setOpen] = useState(false);
  const normalizedCitations = useMemo(
    () => citations.filter((c) => c.source === "web" || c.source === "graph"),
    [citations]
  );

  if (normalizedCitations.length === 0) return null;

  return (
    <div className="mt-3 pt-2 border-t border-border/50 space-y-2">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="text-xs font-medium text-muted-foreground hover:text-foreground"
      >
        {open ? "▴ " : "▾ "}
        {normalizedCitations.length} source{normalizedCitations.length === 1 ? "" : "s"}
      </button>

      {open ? (
        <div className="space-y-2">
          {normalizedCitations.map((c, j) => {
            const sourceLabel = c.source === "graph" ? "Graph" : "Web";
            const href = c.url || undefined;
            const display =
              c.title ||
              (c.source === "graph" ? "Chicago Crime Knowledge Graph" : c.url || c.source) ||
              "Unknown source";
            return (
              <div key={j} className="glass-panel rounded-xl p-3">
                <div className="text-xs text-muted-foreground">{sourceLabel}</div>
                <div className="text-sm font-medium break-words">
                  {href ? (
                    <a href={href} target="_blank" rel="noopener noreferrer" className="text-primary underline">
                      {display}
                    </a>
                  ) : (
                    display
                  )}
                </div>
                {c.detail ? <div className="text-xs text-muted-foreground mt-1">{c.detail.slice(0, 200)}</div> : null}
              </div>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}

function ReasoningToggle({ thinking, steps }: { thinking: string[]; steps: string[] }) {
  const [open, setOpen] = useState(false);
  if (thinking.length === 0 && steps.length === 0) return null;
  return (
    <div className="mb-3 rounded-xl border border-border/50 bg-secondary/20">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="w-full px-3 py-2 text-left text-xs font-medium text-muted-foreground hover:text-foreground"
      >
        {open ? "▴ " : "▾ "}Thinking & steps
      </button>
      {open ? (
        <div className="px-3 pb-3 space-y-2">
          {thinking.length > 0 ? (
            <div>
              <div className="text-[11px] uppercase tracking-wide text-muted-foreground mb-1">Thinking</div>
              <ul className="text-xs space-y-1">
                {thinking.map((t, i) => (
                  <li key={`t-${i}`}>- {t}</li>
                ))}
              </ul>
            </div>
          ) : null}
          {steps.length > 0 ? (
            <div>
              <div className="text-[11px] uppercase tracking-wide text-muted-foreground mb-1">Steps</div>
              <ol className="text-xs space-y-1 list-decimal pl-4">
                {steps.map((s, i) => (
                  <li key={`s-${i}`}>{s}</li>
                ))}
              </ol>
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

function summarizeCollapsedSession(s: ChatSession): string {
  const lastAssistant = [...s.messages].reverse().find((m) => m.role === "assistant");
  if (!lastAssistant) {
    const lastUser = [...s.messages].reverse().find((m) => m.role === "user");
    return lastUser?.content || "No messages yet";
  }
  const thinkingPreview = (lastAssistant.thinking || []).at(-1) || "";
  const stepPreview = (lastAssistant.steps || []).at(-1) || "";
  const resultPreview = (lastAssistant.content || "").replace(/\s+/g, " ").trim().slice(0, 110);
  const parts = [thinkingPreview, stepPreview, resultPreview].filter(Boolean);
  return parts.join(" • ") || "Assistant response";
}

const SUGGESTION_CHIPS: Array<{ label: string; q: string }> = [
  {
    label: "Dangerous areas past few months",
    q: "What are the top 5 highest-risk community areas over the past 3 months?",
  },
  {
    label: "Murder trend past 3 months",
    q: "How has the murder trend changed over the past 3 months?",
  },
  {
    label: "Car thefts by areas",
    q: "Which community areas have the highest number of car thefts recently?",
  },
  {
    label: "Overall theft trend this year",
    q: "Are overall thefts increasing or decreasing over the past year?",
  },
];

function SessionsPanel({
  sessions,
  activeSessionId,
  onSelectSession,
  onNewSession,
  disableSessions,
}: {
  sessions: ChatSession[];
  activeSessionId: string | null;
  onSelectSession: (id: string) => void;
  onNewSession: () => void;
  disableSessions: boolean;
}) {
  return (
    <div className="flex h-full min-h-0 flex-1 flex-col">
      <div className="mb-4 flex shrink-0 items-center justify-between">
        <div className="text-sm font-semibold tracking-tight">Recent Conversations</div>
      </div>

      <div className="min-h-0 flex-1 space-y-2 overflow-y-auto pr-1 scrollbar-thin">
        {sessions.length === 0 ? (
          <div className="text-sm text-muted-foreground">No conversations yet.</div>
        ) : (
          [...sessions].reverse().map((s) => {
            const active = s.id === activeSessionId;
            return (
              <button
                key={s.id}
                type="button"
                disabled={disableSessions}
                onClick={() => onSelectSession(s.id)}
                className={cn(
                  "w-full rounded-xl border px-3 py-2 text-left transition-colors",
                  active
                    ? "border-primary/50 bg-primary/10 text-foreground"
                    : "border-border/50 bg-transparent text-muted-foreground hover:border-primary/40 hover:text-foreground",
                  disableSessions && "opacity-60"
                )}
              >
                <div className="truncate text-xs font-medium">{s.title || "Untitled"}</div>
                <div className="mt-1 line-clamp-3 text-[11px] leading-relaxed text-muted-foreground">
                  {summarizeCollapsedSession(s)}
                </div>
              </button>
            );
          })
        )}
      </div>

      <button
        type="button"
        onClick={onNewSession}
        disabled={disableSessions}
        className="mt-3 w-full shrink-0 rounded-xl bg-primary px-3 py-2 text-sm font-medium text-primary-foreground disabled:opacity-60"
      >
        + New conversation
      </button>
    </div>
  );
}

export default function RagChatbot() {
  const [sessions, setSessions] = useState<ChatSession[]>([]);
  const [activeSessionId, setActiveSessionId] = useState<string | null>(null);

  const [input, setInput] = useState("");
  const [streaming, setStreaming] = useState(false);
  const [activityText, setActivityText] = useState("");

  const [limitsOpen, setLimitsOpen] = useState(false);
  const [historyOpen, setHistoryOpen] = useState(false);

  const wsRef = useRef<WebSocket | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const autoScrollRef = useRef(true);

  useEffect(() => {
    const loaded = loadSessions();
    setSessions(loaded);
    if (loaded.length > 0) setActiveSessionId(loaded[loaded.length - 1].id);
  }, []);

  useEffect(() => {
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(sessions));
    } catch {
      // ignore
    }
  }, [sessions]);

  // Clear the chat sessions when leaving the tab/route so “recent conversations”
  // don’t persist longer than the active browser session.
  useEffect(() => {
    const purge = () => {
      try {
        sessionStorage.removeItem(STORAGE_KEY);
      } catch {
        // ignore
      }
    };

    window.addEventListener("beforeunload", purge);
    return () => {
      window.removeEventListener("beforeunload", purge);
      purge();
    };
  }, []);

  const activeSession = useMemo(
    () => (activeSessionId ? sessions.find((s) => s.id === activeSessionId) || null : null),
    [sessions, activeSessionId]
  );
  const messages = activeSession?.messages ?? [];
  const lastMessageContent = messages.length > 0 ? messages[messages.length - 1]?.content ?? "" : "";

  useEffect(() => {
    // When a new answer starts streaming, we want to keep the chat pinned to the bottom
    // (as long as the user hasn't manually scrolled up).
    if (streaming) autoScrollRef.current = true;
  }, [streaming]);

  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    if (!autoScrollRef.current) return;

    // Avoid scroll/jank during rapid token updates.
    const raf = window.requestAnimationFrame(() => {
      el.scrollTop = el.scrollHeight;
    });
    return () => window.cancelAnimationFrame(raf);
  }, [lastMessageContent]);

  const handleScroll = useCallback(() => {
    const el = scrollRef.current;
    if (!el) return;

    // If the user is close enough to the bottom, keep auto-scrolling enabled.
    const distanceFromBottom = el.scrollHeight - (el.scrollTop + el.clientHeight);
    autoScrollRef.current = distanceFromBottom < 120;
  }, []);

  const startNewSession = useCallback(() => {
    const id = newId();
    const newSession: ChatSession = { id, title: "", messages: [] };
    setSessions((prev) => [...prev, newSession]);
    setActiveSessionId(id);
    return id;
  }, []);

  const sendQuestion = useCallback(
    (overrideQuestion?: string) => {
      const question = (overrideQuestion ?? input).trim();
      if (!question || streaming) return;

      let sessionId = activeSessionId;
      if (!sessionId) sessionId = startNewSession();

      // Close any in-flight websocket to avoid mixing responses.
      try {
        if (wsRef.current) wsRef.current.close();
      } catch {
        // ignore
      }

      setActivityText("Thinking…");
      setStreaming(true);

      // Optimistically write user + assistant placeholder to the active session.
      setSessions((prev) =>
        prev.map((s) =>
          s.id !== sessionId
            ? s
            : {
                ...s,
                title: s.title,
                messages: [
                  ...s.messages,
                  { role: "user", content: question },
                  { role: "assistant", content: "", citations: [], thinking: [], steps: [] },
                ],
              }
        )
      );

      setInput("");

      const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
      const ws = new WebSocket(`${proto}//${window.location.host}/ws/chat`);
      wsRef.current = ws;

      ws.onopen = () => {
        ws.send(JSON.stringify({ question, session_id: sessionId }));
      };

      ws.onmessage = (event) => {
        // If a new websocket was created while this one is still alive,
        // ignore events from the older connection.
        if (wsRef.current !== ws) return;
        try {
          const data = JSON.parse(event.data);

          if (data?.type === "thinking") {
            setActivityText(data.text || "Thinking…");
            setSessions((prev) =>
              prev.map((s) => {
                if (s.id !== sessionId) return s;
                const msgs = [...s.messages];
                const last = msgs[msgs.length - 1];
                if (last?.role === "assistant") {
                  const nextThinking = [...(last.thinking || [])];
                  if (data.text && nextThinking[nextThinking.length - 1] !== data.text) {
                    nextThinking.push(data.text);
                  }
                  msgs[msgs.length - 1] = { ...last, thinking: nextThinking };
                }
                return { ...s, messages: msgs };
              })
            );
            return;
          }
          if (data?.type === "tool_call") {
            setActivityText(data.label || "Working…");
            setSessions((prev) =>
              prev.map((s) => {
                if (s.id !== sessionId) return s;
                const msgs = [...s.messages];
                const last = msgs[msgs.length - 1];
                if (last?.role === "assistant") {
                  const nextSteps = [...(last.steps || [])];
                  const callText = data.input_summary ? `${data.label}: ${data.input_summary}` : data.label;
                  if (callText && nextSteps[nextSteps.length - 1] !== callText) {
                    nextSteps.push(callText);
                  }
                  msgs[msgs.length - 1] = { ...last, steps: nextSteps };
                }
                return { ...s, messages: msgs };
              })
            );
            return;
          }
          if (data?.type === "tool_result") {
            setActivityText(data.label || "Done");
            setSessions((prev) =>
              prev.map((s) => {
                if (s.id !== sessionId) return s;
                const msgs = [...s.messages];
                const last = msgs[msgs.length - 1];
                if (last?.role === "assistant") {
                  const nextSteps = [...(last.steps || [])];
                  const resultText = data.result_summary
                    ? `${data.label}: ${data.result_summary}`
                    : `${data.label}: completed`;
                  if (nextSteps[nextSteps.length - 1] !== resultText) {
                    nextSteps.push(resultText);
                  }
                  msgs[msgs.length - 1] = { ...last, steps: nextSteps };
                }
                return { ...s, messages: msgs };
              })
            );
            return;
          }

          if (data?.type === "token") {
            const tokenText = data.text || data.content || "";
            setSessions((prev) =>
              prev.map((s) => {
                if (s.id !== sessionId) return s;
                const msgs = [...s.messages];
                const last = msgs[msgs.length - 1];
                if (last?.role === "assistant") {
                  msgs[msgs.length - 1] = { ...last, content: last.content + tokenText };
                } else {
                  msgs.push({ role: "assistant", content: tokenText, citations: [], thinking: [], steps: [] });
                }
                return { ...s, messages: msgs };
              })
            );
            return;
          }

          if (data?.type === "done") {
            setStreaming(false);
            setActivityText("");

            const citations: Citation[] = Array.isArray(data.citations)
              ? data.citations.map((c: unknown) => {
                  const citation = c as Partial<Citation>;
                  return {
                    source: citation.source,
                    url: citation.url ?? null,
                    title: citation.title ?? null,
                    detail: citation.detail ?? null,
                  };
                })
              : [];

            setSessions((prev) =>
              prev.map((s) => {
                if (s.id !== sessionId) return s;
                const msgs = [...s.messages];
                const last = msgs[msgs.length - 1];

                const finalAnswer = data.answer || last?.content || "";
                if (last?.role === "assistant") {
                  msgs[msgs.length - 1] = { ...last, content: finalAnswer, citations };
                } else {
                  msgs.push({ role: "assistant", content: finalAnswer, citations });
                }

                const nextTitle = s.title ? s.title : truncateTitle(question);
                return { ...s, title: nextTitle, messages: msgs };
              })
            );
            return;
          }

          if (data?.type === "error") {
            setStreaming(false);
            setActivityText("");
            const detail = data.detail || "Something went wrong.";

            setSessions((prev) =>
              prev.map((s) => {
                if (s.id !== sessionId) return s;
                const msgs = [...s.messages];
                const last = msgs[msgs.length - 1];
                const errText = `Error: ${detail}`;
                if (last?.role === "assistant") {
                  msgs[msgs.length - 1] = { ...last, content: errText, citations: [] };
                } else {
                  msgs.push({ role: "assistant", content: errText, citations: [] });
                }
                return { ...s, messages: msgs };
              })
            );
            return;
          }
        } catch {
          // ignore non-JSON frames
        }
      };

      ws.onclose = () => {
        if (wsRef.current !== ws) return;
        setStreaming(false);
        setActivityText("");
      };

      ws.onerror = () => {
        if (wsRef.current !== ws) return;
        setStreaming(false);
        setActivityText("");
      };
    },
    [activeSessionId, input, streaming, startNewSession]
  );

  const disableSessions = streaming;

  return (
    <div className="animate-fade-in flex min-h-0 flex-1 flex-col overflow-hidden lg:flex-row lg:gap-4">
      <aside className="glass-panel hidden min-h-0 w-72 shrink-0 flex-col rounded-2xl p-4 lg:flex">
        <SessionsPanel
          sessions={sessions}
          activeSessionId={activeSessionId}
          onSelectSession={setActiveSessionId}
          onNewSession={startNewSession}
          disableSessions={disableSessions}
        />
      </aside>

      <Sheet open={historyOpen} onOpenChange={setHistoryOpen}>
        <SheetContent
          side="left"
          className="flex w-[min(100vw-0.5rem,20rem)] flex-col border-border bg-background p-4 sm:max-w-sm"
        >
          <SheetHeader className="sr-only">
            <SheetTitle>Conversations</SheetTitle>
          </SheetHeader>
          <SessionsPanel
            sessions={sessions}
            activeSessionId={activeSessionId}
            onSelectSession={(id) => {
              setActiveSessionId(id);
              setHistoryOpen(false);
            }}
            onNewSession={() => {
              startNewSession();
              setHistoryOpen(false);
            }}
            disableSessions={disableSessions}
          />
        </SheetContent>
      </Sheet>

      <section
        className={cn(
          "flex min-h-0 min-w-0 flex-1 flex-col",
          "px-3 pt-1 sm:px-4",
          "pb-safe",
          "lg:glass-panel lg:rounded-2xl lg:p-4 lg:pb-4"
        )}
      >
        <div className="mb-2 flex shrink-0 items-center justify-between gap-2 lg:mb-3">
          <div className="flex min-w-0 items-center gap-1 sm:gap-2">
            <button
              type="button"
              className="-ml-1 inline-flex h-10 w-10 shrink-0 items-center justify-center rounded-xl border border-border/50 bg-secondary/40 text-foreground hover:bg-secondary/60 lg:hidden"
              aria-label="Open conversation history"
              onClick={() => setHistoryOpen(true)}
            >
              <Menu className="h-5 w-5" />
            </button>
            <div className="truncate text-base font-semibold tracking-tight sm:text-lg">AI Crime Analyst</div>
          </div>
          <button
            type="button"
            onClick={() => setLimitsOpen(true)}
            className="shrink-0 text-xs text-muted-foreground underline hover:text-foreground"
          >
            Limitations
          </button>
        </div>

        <div
          ref={scrollRef}
          onScroll={handleScroll}
          className="min-h-0 flex-1 space-y-4 overflow-y-auto overscroll-contain [-webkit-overflow-scrolling:touch]"
        >
          {messages.length === 0 ? (
            <div className="flex h-full min-h-[12rem] flex-col items-start justify-center px-0.5">
              <div className="text-sm text-muted-foreground">Ask anything about Chicago crime data.</div>

              <div className="mt-4 flex flex-col gap-2 sm:flex-row sm:flex-wrap">
                {SUGGESTION_CHIPS.map((chip) => (
                  <button
                    key={chip.label}
                    type="button"
                    disabled={streaming}
                    onClick={() => sendQuestion(chip.q)}
                    className="rounded-full border border-border/50 bg-secondary/30 px-3 py-2 text-left text-xs text-muted-foreground hover:border-primary/40 hover:text-foreground disabled:opacity-60 sm:py-1.5 sm:text-center"
                  >
                    {chip.label}
                  </button>
                ))}
              </div>

              <div className="mt-4 text-xs leading-relaxed text-muted-foreground">
                ⚠ Data coverage for the AI assistant starts Mar 1, 2025 (older dates unavailable) · Predictions:
                community-area level only ·{" "}
                <button
                  type="button"
                  className="text-primary underline hover:text-primary/90"
                  onClick={() => setLimitsOpen(true)}
                >
                  See all limitations
                </button>
              </div>
            </div>
          ) : (
            messages.map((msg, i) => (
              <div
                key={i}
                className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
              >
                <div
                  className={cn(
                    "max-w-[min(92%,36rem)] rounded-xl px-3 py-3 text-sm leading-relaxed sm:max-w-[80%] sm:px-4",
                    msg.role === "user"
                      ? "bg-primary text-primary-foreground"
                      : "glass-panel text-foreground"
                  )}
                >
                  {msg.role === "assistant" ? (
                    <>
                      <ReasoningToggle thinking={msg.thinking || []} steps={msg.steps || []} />
                      <div className="prose prose-sm prose-invert max-w-none [&_p]:mb-2 [&_p:last-child]:mb-0 [&_table]:not-prose">
                        <ReactMarkdown
                          remarkPlugins={[remarkGfm]}
                          components={{
                            table: Table,
                            thead: TableHeader,
                            tbody: TableBody,
                            tfoot: TableFooter,
                            tr: TableRow,
                            th: TableHead,
                            td: TableCell,
                          }}
                        >
                          {msg.content}
                        </ReactMarkdown>
                      </div>
                    </>
                  ) : (
                    <div className="whitespace-pre-wrap">{msg.content}</div>
                  )}

                  {msg.role === "assistant" && msg.citations && msg.citations.length > 0 ? (
                    <CitationsToggle citations={msg.citations} />
                  ) : null}
                </div>
              </div>
            ))
          )}

          {streaming && messages.length > 0 ? null : null}
        </div>

        {/* Input */}
        <div className="pt-3 border-t border-border/50 mt-3">
          {activityText && streaming ? (
            <div className="flex items-center gap-2 text-xs text-muted-foreground mb-3">
              <span className="w-2 h-2 rounded-full bg-primary animate-pulse" />
              {activityText}
            </div>
          ) : null}

          <div className="flex gap-2 items-end">
            <textarea
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && !e.shiftKey) {
                  e.preventDefault();
                  sendQuestion();
                }
              }}
              placeholder="Ask about Chicago crime data…"
              rows={1}
              maxLength={4000}
              className="flex-1 resize-none rounded-xl bg-secondary/50 border border-border/50 px-4 py-3 text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
            />
            <button
              type="button"
              onClick={() => sendQuestion()}
              disabled={!input.trim() || streaming}
              className="flex items-center justify-center w-10 h-10 rounded-xl bg-primary text-primary-foreground disabled:opacity-60 hover:bg-primary/90 transition-colors"
            >
              <Send className="w-4 h-4" />
            </button>
          </div>

          <div className="flex items-center justify-between mt-2">
            <div className="text-xs text-muted-foreground">{input.length} / 4000</div>
            <button
              type="button"
              className="text-xs text-muted-foreground underline hover:text-foreground"
              onClick={() => setLimitsOpen(true)}
            >
              Limitations
            </button>
          </div>
        </div>
      </section>

      <LimitationsModal open={limitsOpen} onClose={() => setLimitsOpen(false)} />
    </div>
  );
}
