import { useState } from "react";
import LiveMap from "@/components/LiveMap";
import TopNav from "@/components/TopNav";
import LimitationsModal from "@/components/LimitationsModal";

export default function MapPage() {
  const [limitsOpen, setLimitsOpen] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);

  return (
    <div className="flex min-h-dvh flex-col">
      <TopNav variant="dense" />
      <main className="flex min-h-0 flex-1 flex-col px-3 pb-4 sm:px-6 sm:pb-6">
        <div className="flex items-center justify-end gap-3 mb-2">
          <button
            type="button"
            onClick={() => setRefreshKey((k) => k + 1)}
            className="rounded-xl border border-primary/40 bg-primary/12 px-3 py-2 text-xs font-medium text-foreground hover:bg-primary/20"
          >
            ↻ Refresh
          </button>
          <button
            type="button"
            onClick={() => setLimitsOpen(true)}
            className="text-xs text-muted-foreground underline decoration-primary/60 underline-offset-4 hover:text-foreground"
          >
            Limitations
          </button>
        </div>
        <LiveMap key={refreshKey} />
      </main>

      <LimitationsModal open={limitsOpen} onClose={() => setLimitsOpen(false)} />
    </div>
  );
}
