"""Clara brand palette.

Uses the CP AXTRA corporate colour system. Every surface that renders colour —
CLI output, API-served theme tokens, generated dashboards — pulls from here so
the platform looks like one product.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Color:
    """A palette entry, carried with every representation we need."""

    name: str
    r: int
    g: int
    b: int

    @property
    def hex(self) -> str:
        return f"#{self.r:02X}{self.g:02X}{self.b:02X}"

    @property
    def rgb(self) -> tuple[int, int, int]:
        return (self.r, self.g, self.b)

    @property
    def css(self) -> str:
        return f"rgb({self.r}, {self.g}, {self.b})"

    def rich(self) -> str:
        """A Rich-compatible colour token for CLI styling."""
        return self.hex


# ---------------------------------------------------------------- core palette
# CP AXTRA corporate colours.
BLUE = Color("blue", 48, 111, 199)  # CP AXTRA primary
YELLOW = Color("yellow", 246, 194, 74)  # CP AXTRA accent
GREEN = Color("green", 67, 147, 143)  # Lotus
RED = Color("red", 218, 56, 50)  # Makro

PALETTE: dict[str, Color] = {c.name: c for c in (BLUE, YELLOW, GREEN, RED)}

# ------------------------------------------------------------ semantic mapping
# Indirection layer: code refers to intent, not to a colour name, so the palette
# can be re-pointed without touching call sites.
PRIMARY = BLUE
ACCENT = YELLOW
SUCCESS = GREEN
DANGER = RED
WARNING = YELLOW
INFO = BLUE

#: Ordered series colours for charts and multi-series CLI output.
SERIES: tuple[Color, ...] = (BLUE, GREEN, YELLOW, RED)

#: Status token -> semantic colour, shared by the CLI and the API.
STATUS_COLORS: dict[str, Color] = {
    "pending": YELLOW,
    "queued": YELLOW,
    "starting": BLUE,
    "running": BLUE,
    "succeeded": GREEN,
    "success": GREEN,
    "healthy": GREEN,
    "failed": RED,
    "error": RED,
    "cancelled": YELLOW,
    "suspended": YELLOW,
    "stopped": YELLOW,
    "unknown": YELLOW,
}


def status_color(status: str) -> Color:
    """Semantic colour for a run/resource status, defaulting to a neutral accent."""
    return STATUS_COLORS.get(str(status).lower(), ACCENT)


def theme_tokens() -> dict[str, str]:
    """Flat hex token map, served by the API for any UI that renders Clara."""
    tokens = {f"color.{name}": c.hex for name, c in PALETTE.items()}
    tokens.update(
        {
            "color.primary": PRIMARY.hex,
            "color.accent": ACCENT.hex,
            "color.success": SUCCESS.hex,
            "color.danger": DANGER.hex,
            "color.warning": WARNING.hex,
            "color.info": INFO.hex,
        }
    )
    for i, c in enumerate(SERIES):
        tokens[f"color.series.{i}"] = c.hex
    return tokens