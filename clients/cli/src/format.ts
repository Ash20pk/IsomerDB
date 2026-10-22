// Terminal output: aligned tables, a lag bar, and colour only when writing to a terminal.

export interface Style {
  bold(s: string): string;
  dim(s: string): string;
  red(s: string): string;
  yellow(s: string): string;
  green(s: string): string;
}

const ESC = "\u001b[";

export function style(color: boolean): Style {
  const wrap = (code: string) => (s: string) => (color ? `${ESC}${code}m${s}${ESC}0m` : s);
  return { bold: wrap("1"), dim: wrap("2"), red: wrap("31"), yellow: wrap("33"), green: wrap("32") };
}

/** Visible width, ignoring ANSI colour codes. */
export function width(s: string): number {
  // eslint-disable-next-line no-control-regex
  return [...s.replace(/\u001b\[[0-9;]*m/g, "")].length;
}

function pad(s: string, w: number, right: boolean): string {
  const fill = " ".repeat(Math.max(0, w - width(s)));
  return right ? fill + s : s + fill;
}

/**
 * Rows under a header, columns aligned. Columns named in `right` are right-aligned (numbers).
 * Cells longer than `max` are cut with an ellipsis.
 */
export function table(header: string[], rows: string[][], opts: { right?: number[]; max?: number; s?: Style } = {}): string {
  const max = opts.max ?? 60;
  const cut = (c: string) => (width(c) > max ? `${[...c].slice(0, max - 1).join("")}…` : c);
  const cells = rows.map((r) => r.map(cut));
  const widths = header.map((h, i) => Math.max(width(h), ...cells.map((r) => width(r[i] ?? ""))));
  const right = new Set(opts.right ?? []);
  const line = (r: string[]) => r.map((c, i) => pad(c, widths[i] ?? 0, right.has(i))).join("  ").trimEnd();
  const head = line(header);
  return [opts.s ? opts.s.bold(head) : head, ...cells.map(line)].join("\n");
}

/** A ten-cell bar for a ratio of budget used: ▰▰▰▱▱▱▱▱▱▱, red past the budget. */
export function bar(ratio: number | null, s: Style, cells = 10): string {
  if (ratio === null) return s.dim("·".repeat(cells));
  const filled = Math.min(cells, Math.round(Math.min(ratio, 1) * cells));
  const text = "▰".repeat(filled) + "▱".repeat(cells - filled);
  if (ratio > 1) return s.red(text);
  if (ratio > 0.5) return s.yellow(text);
  return s.green(text);
}

export function plural(n: number, word: string): string {
  return `${n} ${word}${n === 1 ? "" : "s"}`;
}
