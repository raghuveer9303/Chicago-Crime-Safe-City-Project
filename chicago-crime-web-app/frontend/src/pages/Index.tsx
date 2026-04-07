import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import TopNav from "@/components/TopNav";
import LimitationsModal from "@/components/LimitationsModal";

type CitySummary = {
  average_safety: number;
  total_areas: number;
  process_date: string;
};

type CityCompare = {
  process_date_current: string;
  process_date_previous: string;
  average_safety_current: number;
  average_safety_previous: number;
  delta_average_safety: number;
  direction: "up" | "down" | "flat" | string;
  total_areas_current: number;
  total_areas_previous: number;
};

function deltaText(direction: CityCompare["direction"], deltaAbs: string) {
  if (direction === "up") return `↑ +${deltaAbs}`;
  if (direction === "down") return `↓ −${deltaAbs}`;
  return `→ ${deltaAbs}`;
}

export default function Index() {
  const [avgSafety, setAvgSafety] = useState<string>("—");
  const [avgSafetySub, setAvgSafetySub] = useState<string>("");

  const [totalAreas, setTotalAreas] = useState<string>("—");
  const [totalAreasSub, setTotalAreasSub] = useState<string>("");

  const [trend, setTrend] = useState<string>("—");
  const [trendSub, setTrendSub] = useState<string>("");

  const [processDate, setProcessDate] = useState<string>("—");
  const [processDateSub, setProcessDateSub] = useState<string>("");

  const [limitsOpen, setLimitsOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function loadDashboard() {
    try {
      setError(null);
      setAvgSafety("—");
      setTotalAreas("—");
      setTrend("—");
      setProcessDate("—");

      const [sumRes, cmpRes] = await Promise.all([
        fetch("/api/ml/city-summary"),
        fetch("/api/ml/city-summary/compare"),
      ]);

      if (!sumRes.ok || !cmpRes.ok) throw new Error("API error");

      const sum = (await sumRes.json()) as CitySummary;
      const cmp = (await cmpRes.json()) as CityCompare;

      setAvgSafety(`${Number(sum.average_safety).toFixed(1)}`);
      setAvgSafetySub(`As of ${sum.process_date}`);

      setTotalAreas(String(sum.total_areas));
      setTotalAreasSub("Community areas tracked");

      const deltaAbs = Math.abs(cmp.delta_average_safety).toFixed(2);
      setTrend(deltaText(cmp.direction, deltaAbs));
      setTrendSub(`Day-over-day change (prev day: ${cmp.process_date_previous})`);

      setProcessDate(sum.process_date);
      setProcessDateSub("Last model run (process_date)");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not load data");
      setAvgSafety("—");
      setTotalAreas("—");
      setTrend("—");
      setProcessDate("—");
      setAvgSafetySub("Could not load data");
      setTotalAreasSub("Could not load data");
      setTrendSub("Could not load data");
      setProcessDateSub("Could not load data");
    }
  }

  useEffect(() => {
    void loadDashboard();
  }, []);

  return (
    <div className="flex min-h-dvh flex-col">
      <TopNav />

      <main className="flex-1 px-4 sm:px-6 pb-6">
        <div className="max-w-6xl mx-auto">
          <div className="flex items-start justify-between gap-4 mb-5">
            <div className="min-w-0">
              <h1 className="text-xl sm:text-2xl font-semibold tracking-tight">City Safety Dashboard</h1>
              <p className="text-sm text-muted-foreground mt-1">
                Civic analytics view of modeled city risk and neighborhood trend movement.
              </p>
            </div>

            <button
              onClick={() => void loadDashboard()}
              className="shrink-0 rounded-xl border border-primary/40 bg-primary/12 px-4 py-2 text-sm font-medium hover:bg-primary/20"
            >
              ↻ Refresh
            </button>
          </div>

          <section className="grid gap-4 lg:grid-cols-12 lg:items-stretch">
            {/* Left column */}
            <div className="grid gap-4 lg:col-span-3">
              <div className="glass-panel rounded-2xl p-5 h-full">
                <div className="text-sm text-muted-foreground">Quick start</div>
                <div className="text-base font-semibold mt-2">Pick your first view</div>
                <div className="mt-3 flex flex-col gap-2">
                  <Link
                    to="/map"
                    className="rounded-xl bg-primary text-primary-foreground px-4 py-2 text-sm font-medium shadow-[0_8px_24px_hsl(var(--primary)/0.24)] hover:bg-primary/90 transition-colors text-center"
                  >
                    Open Live Safety Map
                  </Link>
                  <Link
                    to="/chat"
                    className="rounded-xl border border-primary/30 bg-secondary/60 px-4 py-2 text-sm hover:border-primary/60 transition-colors text-center"
                  >
                    Ask the AI Crime Analyst
                  </Link>
                </div>
                <div className="mt-4 text-xs text-muted-foreground leading-relaxed">
                  Scores are a <strong>0–100 index</strong> (no percent sign).
                </div>
              </div>

              <div className="glass-panel rounded-2xl p-5 h-full">
                <div className="text-sm text-muted-foreground">What the score means</div>
                <div className="text-base font-semibold mt-2">Higher is safer</div>
                <div className="mt-3 text-sm text-muted-foreground leading-relaxed">
                  The map and dashboard visualize a model-derived index built from predicted crime counts.
                  It’s for comparison and pattern discovery, not official safety guarantees.
                </div>
              </div>
            </div>

            {/* Center hero */}
            <div className="glass-panel rounded-2xl p-6 lg:col-span-6 h-full">
              <div className="flex items-start justify-between gap-4">
                <div className="min-w-0">
                  <div className="text-sm text-muted-foreground">Three views of the same story</div>
                  <div className="text-lg sm:text-xl font-semibold mt-1">Dashboard · Map · Chat</div>
                  <p className="text-sm text-muted-foreground mt-3 leading-relaxed">
                    Track citywide Safety Score movement, then drill into community areas on the live map, and finally
                    ask “why” questions in chat.
                  </p>
                  <div className="mt-3 text-xs text-muted-foreground leading-relaxed">
                    Chat lookups are available from <strong>Mar 1, 2025</strong> onward.
                  </div>
                </div>
              </div>

              <div className="grid sm:grid-cols-2 gap-4 mt-6">
                <div className="glass-panel-strong rounded-2xl p-5">
                  <div className="text-sm text-muted-foreground">Average Safety Score</div>
                  <div className="text-3xl font-bold mt-2">{avgSafety}</div>
                  <div className="text-sm text-muted-foreground mt-1">{avgSafetySub}</div>
                </div>

                <div className="glass-panel-strong rounded-2xl p-5">
                  <div className="text-sm text-muted-foreground">Safety Trend</div>
                  <div className="text-3xl font-bold mt-2">{trend}</div>
                  <div className="text-sm text-muted-foreground mt-1">{trendSub}</div>
                </div>

                <div className="glass-panel-strong rounded-2xl p-5">
                  <div className="text-sm text-muted-foreground">Areas Tracked</div>
                  <div className="text-3xl font-bold mt-2">{totalAreas}</div>
                  <div className="text-sm text-muted-foreground mt-1">{totalAreasSub}</div>
                </div>

                <div className="glass-panel-strong rounded-2xl p-5">
                  <div className="text-sm text-muted-foreground">Data Last Updated</div>
                  <div className="text-3xl font-bold mt-2">{processDate}</div>
                  <div className="text-sm text-muted-foreground mt-1">{processDateSub}</div>
                </div>
              </div>
            </div>

            {/* Right column */}
            <div className="grid gap-4 lg:col-span-3">
              <div className="glass-panel rounded-2xl p-5 h-full">
                <div className="text-sm text-muted-foreground">Live map</div>
                <div className="text-base font-semibold mt-2">Community-by-community</div>
                <div className="mt-3 text-sm text-muted-foreground leading-relaxed">
                  Hover areas to see Safety Score plus predicted counts, bounds, and top crimes.
                </div>
              </div>

              <div className="glass-panel rounded-2xl p-5 h-full">
                <div className="text-sm text-muted-foreground">Short-term context</div>
                <div className="text-base font-semibold mt-2">From Mar 2025 onward</div>
                <div className="mt-3 text-sm text-muted-foreground leading-relaxed">
                  Your Safety Score and city trend reflect recent daily processed batches.
                </div>
              </div>

              <div className="glass-panel rounded-2xl p-5 h-full">
                <div className="text-sm text-muted-foreground">Limitations</div>
                <div className="mt-3 text-sm text-muted-foreground leading-relaxed">
                  ⚠️ <strong>Model-assisted estimates</strong> (~9-day lag). Not an official CPD metric.
                </div>
                <div className="mt-3">
                  <button className="underline text-primary hover:text-primary/90" onClick={() => setLimitsOpen(true)}>
                    See full limitations
                  </button>
                </div>
                {error ? <div className="text-destructive text-sm mt-2">{error}</div> : null}
              </div>
            </div>
          </section>
        </div>
      </main>

      <LimitationsModal open={limitsOpen} onClose={() => setLimitsOpen(false)} />
    </div>
  );
}
