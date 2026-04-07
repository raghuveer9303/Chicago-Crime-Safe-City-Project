import React from "react";

export default function LimitationsModal({
  open,
  onClose,
}: {
  open: boolean;
  onClose: () => void;
}) {
  if (!open) return null;

  return (
    <div className="fixed inset-0 z-[2000] flex items-center justify-center bg-black/60 backdrop-blur-sm p-4">
      <div
        className="glass-panel-strong w-full max-w-4xl rounded-2xl border border-primary/35 p-6"
        role="dialog"
        aria-modal="true"
      >
        <div className="flex items-start justify-between gap-4">
          <div>
            <h2 className="text-xl font-semibold tracking-tight">Data &amp; Model Limitations</h2>
            <p className="mt-1 text-sm text-muted-foreground">
              Guidance on what this tool can and cannot do.
            </p>
          </div>
          <button
            onClick={onClose}
            className="rounded-lg border border-border/70 px-3 py-1 text-sm text-muted-foreground hover:text-foreground"
            aria-label="Close"
          >
            ✕
          </button>
        </div>

        <div className="mt-5 space-y-6 overflow-auto max-h-[70vh] pr-1">
          <section className="space-y-2">
            <h3 className="text-base font-semibold text-foreground">Data Coverage</h3>
            <ul className="list-disc pl-5 text-sm text-muted-foreground space-y-1">
              <li>
                For the AI chatbot, crime records are available only from <strong>Mar 1, 2025 → present</strong>.
              </li>
              <li>
                There is an approximate <strong>9-day ingestion lag</strong> — the most recent ~9 days of incidents
                may not yet be reflected in the database or ML features.
              </li>
              <li>Data is updated daily via an automated Airflow pipeline after the silver ETL completes.</li>
            </ul>
          </section>

          <section className="space-y-2">
            <h3 className="text-base font-semibold text-foreground">ML Predictions</h3>
            <ul className="list-disc pl-5 text-sm text-muted-foreground space-y-1">
              <li>
                Predictions are generated at <strong>community-area level only</strong> (77 areas). District- or
                block-level predictions are not supported.
              </li>
              <li>
                The model forecasts a <strong>rolling 11-day horizon</strong> (today + 10 days). Beyond this window,
                predictions are not available.
              </li>
              <li>
                The model is an <strong>XGBoost regressor</strong> retrained <em>weekly</em>. Between retrains,
                predictions do not adapt to sudden urban events (festivals, major incidents, etc.).
              </li>
              <li>
                Predictions use forward-filled features for future dates — they assume recent crime patterns persist.
              </li>
              <li>
                <strong>Safety Score</strong> is a <strong>0–100 index</strong> (higher = safer) derived from predicted
                crime counts. It is <em>not</em> an official CPD metric.
              </li>
              <li>
                Prediction intervals (low / high) are 90% Poisson confidence intervals based on the predicted count.
                Actual counts will fall outside these bounds ~10% of the time.
              </li>
            </ul>
          </section>

          <section className="space-y-2">
            <h3 className="text-base font-semibold text-foreground">AI Chatbot (RAG Analyst)</h3>
            <ul className="list-disc pl-5 text-sm text-muted-foreground space-y-1">
              <li>
                The chatbot uses an internal crime database populated from historical Chicago crime data. It cannot
                access real-time CPD dispatch records or live incident feeds.
              </li>
              <li>
                The AI may answer questions about crime patterns, trends, categories, and community areas. It{" "}
                <strong>cannot</strong> look up individual case IDs or victim information.
              </li>
              <li>
                Web search is used to supplement answers with external context, but results may not always be current
                or accurate.
              </li>
              <li>
                If you request numeric results for dates earlier than <strong>Mar 1, 2025</strong>, the assistant
                will decline and explain that those dates are outside its available coverage window.
              </li>
              <li>Responses are AI-generated and may contain errors. Always cross-reference official CPD sources.</li>
              <li>
                The model does <strong>not</strong> have access to today's predictions or the safety scores shown on the
                dashboard — those come from the backend API separately.
              </li>
            </ul>
          </section>

          <section className="space-y-2">
            <h3 className="text-base font-semibold text-foreground">General</h3>
            <ul className="list-disc pl-5 text-sm text-muted-foreground space-y-1">
              <li>
                This tool is for <strong>analytical and research purposes only</strong>. It is not intended for
                law-enforcement operations, judicial proceedings, or personal safety decisions.
              </li>
              <li>
                Crime incident data reflects <em>reported</em> crimes only. Unreported crimes are not captured.
              </li>
              <li>
                Geographic boundaries use 2010 Chicago community area definitions and do not reflect any city
                redesignations after that date.
              </li>
            </ul>
          </section>
        </div>
      </div>
    </div>
  );
}

