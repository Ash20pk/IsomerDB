// Pieces every page shares: the freshness track, toasts and error text.

import { useCallback, useRef, useState } from "preact/hooks";

import { IsomerError, currentLagMs, formatMs, lagRatio, type ProjectionStatus } from "@isomerdb/sdk";

export type Sample = { t: number; lag: number };
export type History = Map<string, Sample[]>;

export function useToast(): [string | null, (msg: string) => void] {
  const [msg, setMsg] = useState<string | null>(null);
  const timer = useRef<number>();
  const show = useCallback((m: string) => {
    setMsg(m);
    clearTimeout(timer.current);
    timer.current = window.setTimeout(() => setMsg(null), 5000);
  }, []);
  return [msg, show];
}

export function describe(e: unknown): string {
  if (e instanceof IsomerError) return e.code ? `${e.message} (${e.code})` : e.message;
  return e instanceof Error ? e.message : String(e);
}

/** Lag travelling along a line toward the budget mark; past it the line turns carmine. */
export function Track({ p }: { p: ProjectionStatus }) {
  if (p.freshness === "sync") {
    return (
      <div class="track sync" role="img" aria-label="sync: updated in the writing transaction">
        <div class="rail-line" />
      </div>
    );
  }
  const lag = currentLagMs(p);
  const budget = p.lag_budget_ms ?? 0;
  const scale = Math.max(budget * 1.5, lag, 1);
  const ratio = lagRatio(p) ?? 0;
  const cls = ratio > 1 ? "over" : ratio > 0.5 ? "near" : "";
  const at = `${Math.min(100, (lag / scale) * 100)}%`;
  return (
    <div class={`track ${cls}`} role="img" aria-label={`lag ${formatMs(lag)} of a ${formatMs(budget)} budget`}>
      <div class="rail-line" />
      <div class="lag" style={{ width: at }} />
      <div class="budget" style={{ left: `${(budget / scale) * 100}%` }} data-label={`budget ${formatMs(budget)}`} />
      <div class="dot" style={{ left: at }} />
    </div>
  );
}

