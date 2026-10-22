"""A small C4 diagram renderer: people, systems, containers and components as SVG, in a light and a
dark theme, each element marked built, partly built or planned.

    python3 docs/diagrams/generate.py   # writes docs/diagrams/*.svg

Layouts are explicit (coordinates in generate.py): diagrams are few and read better placed by hand.
"""

from __future__ import annotations

import html
from dataclasses import dataclass, field

FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"

THEMES = {
    "light": {
        "bg": "#ffffff", "ink": "#1b2430", "muted": "#5f6b78", "line": "#6b7682",
        "accent": "#2f5bd3", "accent_fill": "#eaf0fd", "accent_ink": "#16357f",
        "person": "#2f5bd3", "person_ink": "#ffffff",
        "ext_fill": "#f1f3f5", "ext_line": "#9aa4ae",
        "planned_fill": "#ffffff", "planned_line": "#8a96a3",
        "partial": "#b26a00", "partial_fill": "#fff4e0",
        "boundary": "#8a96a3", "label_bg": "#ffffff",
    },
    "dark": {
        "bg": "#0d1117", "ink": "#e6edf3", "muted": "#9aa6b2", "line": "#8b949e",
        "accent": "#6b8de8", "accent_fill": "#13213d", "accent_ink": "#c9d7fb",
        "person": "#6b8de8", "person_ink": "#0d1117",
        "ext_fill": "#161b22", "ext_line": "#4d5966",
        "planned_fill": "#0d1117", "planned_line": "#6e7a87",
        "partial": "#e0a93a", "partial_fill": "#2a2113",
        "boundary": "#6e7a87", "label_bg": "#0d1117",
    },
}

# Average glyph widths (px per character) for wrapping; slightly generous so text never overflows.
CHAR_W = {12: 6.5, 12.5: 6.8, 13: 7.1, 15: 8.7, 11: 6.1}


def wrap(text: str, width_px: float, size: float) -> list[str]:
    per = max(8, int(width_px / CHAR_W.get(size, size * 0.55)))
    lines, cur = [], ""
    for word in text.split():
        if cur and len(cur) + 1 + len(word) > per:
            lines.append(cur)
            cur = word
        else:
            cur = f"{cur} {word}".strip()
    if cur:
        lines.append(cur)
    return lines


@dataclass
class Element:
    id: str
    kind: str          # person | system | container | component | external | db
    title: str
    x: float
    y: float
    w: float
    tech: str = ""
    desc: str = ""
    status: str = "built"   # built | partial | planned
    h: float = 0.0
    _lines: list[str] = field(default_factory=list)

    def layout(self) -> None:
        self._lines = wrap(self.desc, self.w - 28, 12.5) if self.desc else []
        top = 54 if self.kind == "person" else 0
        typeline = bool(self.tech) or self.kind in ("person", "system", "external")
        body = 30 + (18 if typeline else 0) + 8 + len(self._lines) * 17 + 16
        if self.status != "built":
            body += 22   # room for the status badge
        self.h = top + body

    # Anchor points on the element's edge, for relationship lines.
    def anchor(self, side: str, frac: float = 0.5) -> tuple[float, float]:
        top = self.y + (54 if self.kind == "person" else 0)
        if side == "top":
            on_head = self.kind == "person" and abs(frac - 0.5) < 0.05
            return (self.x + self.w * frac, self.y if on_head or self.kind != "person" else self.y + 50)
        if side == "bottom":
            return (self.x + self.w * frac, self.y + self.h)
        if side == "left":
            return (self.x, top + (self.y + self.h - top) * frac)
        return (self.x + self.w, top + (self.y + self.h - top) * frac)


@dataclass
class Rel:
    a: tuple[str, str, float]
    b: tuple[str, str, float]
    label: str = ""
    tech: str = ""
    status: str = "built"
    via: list[tuple[float, float]] = field(default_factory=list)   # bend points
    label_at: float = 0.5        # position along the path (0..1)
    label_dx: float = 0
    label_dy: float = 0
    label_w: float = 190


@dataclass
class Boundary:
    title: str
    x: float
    y: float
    w: float
    h: float
    subtitle: str = ""


class Diagram:
    def __init__(self, title: str, subtitle: str, width: int, height: int):
        self.title, self.subtitle, self.width, self.height = title, subtitle, width, height
        self.elements: dict[str, Element] = {}
        self.rels: list[Rel] = []
        self.boundaries: list[Boundary] = []

    def add(self, el: Element) -> Element:
        el.layout()
        self.elements[el.id] = el
        return el

    def rel(self, a, b, label="", tech="", status="built", via=None, label_at=0.5, label_dx=0, label_dy=0, label_w=190):
        norm = lambda e: e if isinstance(e, tuple) and len(e) == 3 else (e[0], e[1], 0.5)
        self.rels.append(Rel(norm(a), norm(b), label, tech, status, via or [], label_at, label_dx, label_dy, label_w))

    def boundary(self, *args, **kw):
        self.boundaries.append(Boundary(*args, **kw))

    # ── rendering ──────────────────────────────────────────────────────────

    def render(self, theme: str) -> str:
        t = THEMES[theme]
        out = [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.width}" height="{self.height}" '
            f'viewBox="0 0 {self.width} {self.height}" role="img" aria-label="{html.escape(self.title)}">',
            f"<title>{html.escape(self.title)}</title>",
            "<defs>",
            f'<marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse">'
            f'<path d="M0,0 L10,5 L0,10 z" fill="{t["line"]}"/></marker>',
            "</defs>",
            f'<rect width="100%" height="100%" fill="{t["bg"]}"/>',
            f'<text x="32" y="44" font-family="{FONT}" font-size="22" font-weight="650" fill="{t["ink"]}">{html.escape(self.title)}</text>',
            f'<text x="32" y="68" font-family="{FONT}" font-size="13.5" fill="{t["muted"]}">{html.escape(self.subtitle)}</text>',
        ]
        for b in self.boundaries:
            out.append(
                f'<rect x="{b.x}" y="{b.y}" width="{b.w}" height="{b.h}" rx="14" fill="none" stroke="{t["boundary"]}" '
                f'stroke-width="1.5" stroke-dasharray="8 6"/>')
        labels = []
        for r in self.rels:
            out.append(self._rel(r, t, labels))
        for b in self.boundaries:
            # Titles go over the lines, on a background, so a line passing behind never crosses them.
            tw = len(b.title) * 8.6 + (len(b.subtitle) * 6.9 + 10 if b.subtitle else 0) + 16
            out.append(f'<rect x="{b.x + 14}" y="{b.y - 11}" width="{tw}" height="22" fill="{t["bg"]}"/>')
            out.append(f'<text x="{b.x + 22}" y="{b.y + 5}" font-family="{FONT}" font-size="14" font-weight="650" '
                       f'fill="{t["muted"]}">{html.escape(b.title)}</text>')
            if b.subtitle:
                out.append(f'<text x="{b.x + 22 + len(b.title) * 8.6 + 10}" y="{b.y + 5}" font-family="{FONT}" '
                           f'font-size="12.5" fill="{t["muted"]}">{html.escape(b.subtitle)}</text>')
        for el in self.elements.values():
            out.append(self._element(el, t))
        out.extend(labels)
        out.append(self._legend(t))
        out.append("</svg>")
        return "\n".join(out)

    def _points(self, r: Rel) -> list[tuple[float, float]]:
        a = self.elements[r.a[0]].anchor(r.a[1], r.a[2])
        b = self.elements[r.b[0]].anchor(r.b[1], r.b[2])
        return [a, *r.via, b]

    def _rel(self, r: Rel, t: dict, labels: list) -> str:
        pts = self._points(r)
        d = "M" + " L".join(f"{x:.1f},{y:.1f}" for x, y in pts)
        dash = ' stroke-dasharray="6 5"' if r.status == "planned" else ""
        line = (f'<path d="{d}" fill="none" stroke="{t["line"]}" stroke-width="1.4"{dash} '
                f'marker-end="url(#arrow)"/>')
        if r.label:
            # The point label_at of the way along the path.
            segs = list(zip(pts, pts[1:]))
            lens = [((x2 - x1) ** 2 + (y2 - y1) ** 2) ** 0.5 for (x1, y1), (x2, y2) in segs]
            target, acc = sum(lens) * r.label_at, 0.0
            lx, ly = pts[0]
            for ((x1, y1), (x2, y2)), ln in zip(segs, lens):
                if acc + ln >= target and ln > 0:
                    f = (target - acc) / ln
                    lx, ly = x1 + (x2 - x1) * f, y1 + (y2 - y1) * f
                    break
                acc += ln
            lx += r.label_dx
            ly += r.label_dy
            lines = wrap(r.label, r.label_w, 12)
            if r.tech:
                lines.append(f"[{r.tech}]")
            w = max(len(s) for s in lines) * CHAR_W[12] + 12
            h = len(lines) * 15 + 6
            colour = t["muted"] if r.status == "planned" else t["ink"]
            parts = [f'<rect x="{lx - w / 2:.1f}" y="{ly - h / 2:.1f}" width="{w:.1f}" height="{h}" rx="4" '
                     f'fill="{t["label_bg"]}" opacity="0.94"/>']
            for i, s in enumerate(lines):
                is_tech = r.tech and i == len(lines) - 1
                parts.append(
                    f'<text x="{lx:.1f}" y="{ly - h / 2 + 15 + i * 15:.1f}" text-anchor="middle" font-family="{FONT}" '
                    f'font-size="12" fill="{t["muted"] if is_tech else colour}"'
                    f'{" font-style=\"italic\"" if is_tech else ""}>{html.escape(s)}</text>')
            labels.append("\n".join(parts))
        return line

    def _element(self, el: Element, t: dict) -> str:
        external = el.kind == "external"
        planned = el.status == "planned"
        if el.kind == "person":
            fill, stroke, ink, sub = t["person"], t["person"], t["person_ink"], t["person_ink"]
        elif external:
            fill, stroke, ink, sub = t["ext_fill"], t["ext_line"], t["ink"], t["muted"]
        elif planned:
            fill, stroke, ink, sub = t["planned_fill"], t["planned_line"], t["ink"], t["muted"]
        else:
            fill, stroke, ink, sub = t["accent_fill"], t["accent"], t["accent_ink"] if el.kind != "system" else t["ink"], t["muted"]
        dash = ' stroke-dasharray="7 5"' if planned else ""
        parts = []
        top = el.y
        if el.kind == "person":
            cx = el.x + el.w / 2
            parts.append(f'<circle cx="{cx}" cy="{el.y + 24}" r="22" fill="{fill}"/>')
            top = el.y + 50
        h = el.y + el.h - top
        rx = 24 if el.kind == "person" else 10
        sw = 2.2 if el.kind == "system" else 1.6
        parts.append(f'<rect x="{el.x}" y="{top}" width="{el.w}" height="{h}" rx="{rx}" fill="{fill}" '
                     f'stroke="{stroke}" stroke-width="{sw}"{dash}/>')
        if el.kind == "db":   # a cylinder lid for data stores
            parts.append(f'<ellipse cx="{el.x + el.w / 2}" cy="{top}" rx="{el.w / 2}" ry="9" fill="{fill}" stroke="{stroke}" stroke-width="{sw}"{dash}/>')
        cx = el.x + el.w / 2
        y = top + 28
        parts.append(f'<text x="{cx}" y="{y}" text-anchor="middle" font-family="{FONT}" font-size="15" '
                     f'font-weight="650" fill="{ink}">{html.escape(el.title)}</text>')
        kind_label = {"person": "Person", "system": "Software system", "container": "Container",
                      "component": "Component", "external": "External system", "db": "Container: data"}[el.kind]
        tech = f"[{kind_label}{': ' + el.tech if el.tech else ''}]"
        if el.tech or el.kind in ("person", "system", "external"):
            y += 18
            parts.append(f'<text x="{cx}" y="{y}" text-anchor="middle" font-family="{FONT}" font-size="12" '
                         f'font-style="italic" fill="{sub}">{html.escape(tech)}</text>')
        y += 8
        for line in el._lines:
            y += 17
            parts.append(f'<text x="{cx}" y="{y}" text-anchor="middle" font-family="{FONT}" font-size="12.5" '
                         f'fill="{ink if el.kind == "person" else (t["ink"] if not planned else t["muted"])}">{html.escape(line)}</text>')
        if el.status != "built":
            label = "planned" if planned else "partly built"
            colour = t["muted"] if planned else t["partial"]
            bg = t["bg"] if planned else t["partial_fill"]
            w = len(label) * 6.4 + 16
            bx, by = el.x + el.w - w - 8, el.y + el.h - 26
            parts.append(f'<rect x="{bx}" y="{by}" width="{w}" height="18" rx="9" fill="{bg}" stroke="{colour}" stroke-width="1"/>')
            parts.append(f'<text x="{bx + w / 2}" y="{by + 13}" text-anchor="middle" font-family="{FONT}" font-size="11" '
                         f'fill="{colour}">{label}</text>')
        return "\n".join(parts)

    def _legend(self, t: dict) -> str:
        items = [
            ("Built and tested", t["accent_fill"], t["accent"], ""),
            ("Partly built", t["partial_fill"], t["partial"], ""),
            ("Planned", t["planned_fill"], t["planned_line"], ' stroke-dasharray="6 4"'),
            ("External", t["ext_fill"], t["ext_line"], ""),
        ]
        widths = [30 + len(label) * 6.9 + 22 for label, *_ in items]
        x, y = self.width - 32 - sum(widths), 30
        parts = []
        for (label, fill, stroke, dash), w in zip(items, widths):
            parts.append(f'<rect x="{x}" y="{y}" width="22" height="16" rx="4" fill="{fill}" stroke="{stroke}" stroke-width="1.5"{dash}/>')
            parts.append(f'<text x="{x + 30}" y="{y + 12.5}" font-family="{FONT}" font-size="12.5" fill="{t["muted"]}">{label}</text>')
            x += w
        return "\n".join(parts)
