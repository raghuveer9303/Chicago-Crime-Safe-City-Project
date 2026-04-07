import RagChatbot from "@/components/RagChatbot";
import TopNav from "@/components/TopNav";

export default function ChatPage() {
  return (
    <div className="flex h-dvh min-h-dvh flex-col overflow-hidden">
      <TopNav variant="dense" />
      <main className="flex min-h-0 flex-1 flex-col overflow-hidden px-0 md:px-6 md:pb-6">
        <section className="glass-panel mx-4 mt-4 hidden shrink-0 rounded-2xl p-4 sm:p-5 md:mx-0 md:mt-0 md:block">
          <h1 className="text-base font-semibold tracking-tight sm:text-lg">About AI Chat Analyst</h1>
          <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
            Ask questions about crime patterns, trends, categories, and community areas. The assistant uses our crime
            database records (coverage from <strong>Mar 1, 2025 onward</strong>) and may add web context with cited
            sources.
          </p>
          <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
            It does not provide case-level personal details and should be used for analysis, not as an official CPD
            decision source. Live map/dashboard safety scores and predictions are served by backend ML APIs separately.
          </p>
        </section>
        <div className="flex min-h-0 flex-1 flex-col overflow-hidden md:pt-4">
          <RagChatbot />
        </div>
      </main>
    </div>
  );
}
