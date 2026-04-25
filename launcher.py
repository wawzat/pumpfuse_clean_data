#!/usr/bin/env python3
"""
launcher.py — PumpFuse Data Pipeline CLI Launcher.

Provides a curses-based TUI with a persistent menu header (title bar,
numbered menu, and dividing lines) and a scrolling output area below.
Each pipeline script is run as a subprocess; its combined stdout/stderr
streams line-by-line into the display.

After import.py completes, the row number it reports is parsed and the
value (row − 1) is automatically pre-filled as the start-row argument
when the user subsequently runs clean.py.

Usage:
    python launcher.py
"""

import curses
import queue
import re
import subprocess
import sys
import threading
from collections import deque
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent

TITLE = " PumpFuse Data Pipeline "
MENU_OPTIONS: tuple[tuple[int, str], ...] = (
    (1, "GetLooker"),
    (2, "Import"),
    (3, "Clean"),
    (4, "GetWeather"),
    (5, "Exit"),
)

# Matches ANSI CSI escape sequences (colours, cursor movement, etc.) and bare \r
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]|\x1b[()][A-Z0-9]|\r")

# Matches the row number printed by import.py:
#   "Latest datetime in 'Timestamp' column: ... (row 145)"
IMPORT_ROW_RE = re.compile(r"\(row\s+(\d+)\)", re.IGNORECASE)

# Fixed row counts for header and footer areas
_HEADER_ROWS = 3   # row 0: title, row 1: menu, row 2: top divider
_FOOTER_ROWS = 2   # row h-2: bottom divider, row h-1: status/input


# ---------------------------------------------------------------------------
# Launcher
# ---------------------------------------------------------------------------

class Launcher:
    """Curses-based TUI launcher for the PumpFuse pipeline scripts."""

    def __init__(self, stdscr: "curses.window") -> None:
        """Initialise the launcher with the root curses window."""
        self.stdscr = stdscr
        self.output_lines: deque[str] = deque(maxlen=2000)
        self.last_import_row: int | None = None
        self.completed_steps: set[int] = set()
        self._init_curses()

    # ------------------------------------------------------------------
    # Curses configuration
    # ------------------------------------------------------------------

    def _init_curses(self) -> None:
        """Configure curses display settings and colour pairs."""
        curses.curs_set(0)
        if curses.has_colors():
            curses.start_color()
            curses.use_default_colors()
            curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_CYAN)  # title bar
            curses.init_pair(2, curses.COLOR_CYAN, -1)                  # menu items
            curses.init_pair(3, curses.COLOR_YELLOW, -1)                # status line
        self.stdscr.keypad(True)

    # ------------------------------------------------------------------
    # Layout helpers
    # ------------------------------------------------------------------

    def _dims(self) -> tuple[int, int]:
        """Return current terminal (height, width)."""
        return self.stdscr.getmaxyx()

    def _output_region(self) -> tuple[int, int]:
        """Return (first_row, exclusive_last_row) of the scrolling output area."""
        h, _ = self._dims()
        return _HEADER_ROWS, h - _FOOTER_ROWS

    def _is_enter(self, ch: int) -> bool:
        """Return True if *ch* is any Enter-family key on any curses backend.

        Covers the standard newline/carriage-return codes (10, 13) and
        ``curses.KEY_ENTER``, plus the PDCurses constants used on Windows
        (``PADENTER``, ``CTL_PADENTER``, ``ALT_PADENTER``, ``SHF_PADENTER``)
        when the active backend exposes them.
        """
        enter_codes = {curses.KEY_ENTER, 10, 13}
        for name in ("PADENTER", "CTL_PADENTER", "ALT_PADENTER", "SHF_PADENTER"):
            code = getattr(curses, name, None)
            if code is not None:
                enter_codes.add(code)
        return ch in enter_codes

    # ------------------------------------------------------------------
    # Drawing primitives
    # ------------------------------------------------------------------

    def _addstr(self, y: int, x: int, text: str, attr: int = 0) -> None:
        """Safe addstr — silently ignores writes past the screen edge."""
        try:
            self.stdscr.addstr(y, x, text, attr)
        except curses.error:
            pass

    def draw_header(self) -> None:
        """Render the fixed title bar, menu row, and both dividers."""
        h, w = self._dims()
        title_attr = (curses.color_pair(1) | curses.A_BOLD) if curses.has_colors() else curses.A_BOLD
        menu_attr = curses.color_pair(2) if curses.has_colors() else curses.A_NORMAL

        self._addstr(0, 0, TITLE.center(w - 1), title_attr)
        self._addstr(1, 0, " " * (w - 1), menu_attr)
        col = 2
        for option, label in MENU_OPTIONS:
            segment = f"[{option}] {label}"
            attr = menu_attr | curses.A_BOLD if option in self.completed_steps else menu_attr
            self._addstr(1, col, segment[: max(0, w - 1 - col)], attr)
            col += len(segment) + 3
        self._addstr(2, 0, "─" * (w - 1))
        self._addstr(h - 2, 0, "─" * (w - 1))

    def draw_status(self, msg: str = "") -> None:
        """Render the status / input bar on the bottom row."""
        h, w = self._dims()
        attr = curses.color_pair(3) if curses.has_colors() else curses.A_NORMAL
        self._addstr(h - 1, 0, msg.ljust(w - 1)[:w - 1], attr)

    def draw_output(self) -> None:
        """Re-render the scrolling output area from the output_lines deque."""
        h, w = self._dims()
        top, bottom = self._output_region()
        visible = bottom - top
        if visible <= 0:
            return

        lines = list(self.output_lines)[-visible:]
        for i in range(visible):
            row = top + i
            if row >= h - 2:
                break
            text = lines[i][:w - 1].ljust(w - 1) if i < len(lines) else " " * (w - 1)
            self._addstr(row, 0, text)

    def append_output(self, line: str) -> None:
        """Strip ANSI codes, add a line to the deque, and refresh the output area."""
        cleaned = ANSI_ESCAPE.sub("", line).rstrip()
        self.output_lines.append(cleaned)
        self.draw_output()
        self.stdscr.refresh()

    def _refresh_screen(self, status: str = "Enter choice (1-5): ") -> None:
        """Erase and fully redraw all UI regions."""
        self.stdscr.erase()
        self.draw_header()
        self.draw_output()
        self.draw_status(status)
        self.stdscr.refresh()

    # ------------------------------------------------------------------
    # Subprocess runner
    # ------------------------------------------------------------------

    def run_script(self, script_name: str, args: list[str] | None = None) -> list[str]:
        """Run a pipeline script, streaming its output into the TUI.

        The script's stdout and stderr are merged and shown line-by-line in
        the output area as they arrive.  A background reader thread feeds a
        Queue; the main loop drains the queue so the UI stays responsive.

        Parameters
        ----------
        script_name:
            Filename of the script relative to SCRIPT_DIR (e.g. ``import.py``).
        args:
            Optional extra command-line arguments to append.

        Returns
        -------
        list[str]
            All captured output lines (ANSI stripped, no trailing whitespace).
        """
        script_path = SCRIPT_DIR / script_name
        cmd = [sys.executable, str(script_path)] + (args or [])
        self.append_output(f"$ {' '.join(cmd)}")

        captured: list[str] = []
        line_queue: queue.Queue[str | None] = queue.Queue()

        def _reader(proc: subprocess.Popen) -> None:
            """Read subprocess output lines and push them to the queue."""
            try:
                for raw in iter(proc.stdout.readline, ""):
                    line_queue.put(raw)
            except OSError:
                pass
            finally:
                line_queue.put(None)  # sentinel: signals end of output

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                cwd=str(SCRIPT_DIR),
            )
        except OSError as exc:
            self.append_output(f"[ERROR] Failed to start {script_name}: {exc}")
            return captured

        reader_thread = threading.Thread(target=_reader, args=(proc,), daemon=True)
        reader_thread.start()

        self.stdscr.nodelay(True)
        try:
            done = False
            while not done:
                # Drain up to 50 lines per iteration to stay responsive
                for _ in range(50):
                    try:
                        raw = line_queue.get_nowait()
                    except queue.Empty:
                        break
                    if raw is None:
                        done = True
                        break
                    clean = ANSI_ESCAPE.sub("", raw).rstrip()
                    if clean:
                        captured.append(clean)
                        self.append_output(clean)

                if not done:
                    curses.napms(30)
        finally:
            self.stdscr.nodelay(False)

        reader_thread.join(timeout=5)
        proc.wait()
        return captured

    # ------------------------------------------------------------------
    # Clean row prompt
    # ------------------------------------------------------------------

    def _prompt_clean_row(self) -> int | None:
        """Prompt the user for the clean.py start row, pre-filling the auto value.

        The auto value is ``last_import_row`` (set after import.py runs).
        Returns the validated integer row number, or ``None`` if the operation
        should be aborted.
        """
        auto = self.last_import_row
        if auto is not None:
            prompt = f"Start row [auto: {auto}] (Enter=confirm, or type a number): "
        else:
            prompt = "Start row (no auto value — type a number, or Enter to cancel): "

        h, w = self._dims()
        self.draw_header()
        self.draw_status(prompt)
        self.stdscr.refresh()

        # Position the cursor right after the prompt text
        input_col = min(len(prompt), w - 10)
        chars: list[str] = []
        curses.curs_set(1)
        try:
            while True:
                current = "".join(chars)
                self.draw_status((prompt + current)[: w - 1])
                self.stdscr.move(h - 1, min(input_col + len(current), w - 2))
                self.stdscr.refresh()

                ch = self.stdscr.getch()

                if self._is_enter(ch):
                    break

                if ch in (curses.KEY_BACKSPACE, 127, 8):
                    if chars:
                        chars.pop()
                    continue

                if 32 <= ch < 256:
                    chars.append(chr(ch))
        except curses.error:
            chars = []
        finally:
            curses.curs_set(0)

        text = "".join(chars).strip()

        if text == "":
            if auto is None:
                self.append_output(
                    "[INFO] No row number entered and no auto value available. "
                    "Run Import first, or type a row number."
                )
                return None
            return auto

        try:
            value = int(text)
        except ValueError:
            self.append_output(
                f"[ERROR] '{text}' is not a valid integer. Clean aborted."
            )
            return None

        if value <= 0:
            self.append_output(
                f"[ERROR] Row number must be a positive integer (got {value}). "
                "Clean aborted."
            )
            return None

        return value

    # ------------------------------------------------------------------
    # Main event loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Draw the UI and dispatch numbered key presses to pipeline scripts."""
        self._refresh_screen()

        while True:
            self._refresh_screen()
            ch = self.stdscr.getch()
            if self._is_enter(ch):
                key = "\n"
            elif 0 < ch < 256:
                key = chr(ch)
            else:
                key = ""

            if key in ("5", "q", "Q"):
                break

            elif key == "1":
                self.append_output("─── GetLooker ─────────────────────────────────────")
                self.run_script("getlooker.py")
                self.append_output("─── GetLooker complete ────────────────────────────")
                self.completed_steps.add(1)

            elif key == "2":
                self.append_output("─── Import ────────────────────────────────────────")
                captured = self.run_script("import.py")

                # Parse the row number from import.py's output and store (row − 1)
                # as the default start row for clean.py.
                for line in reversed(captured):
                    m = IMPORT_ROW_RE.search(line)
                    if m:
                        import_row = int(m.group(1))
                        self.last_import_row = import_row - 1
                        self.append_output(
                            f"[Launcher] Clean start row auto-set to "
                            f"{self.last_import_row} "
                            f"(import row {import_row} \u2212 1)"
                        )
                        break

                self.append_output("─── Import complete ───────────────────────────────")
                self.completed_steps.add(2)

            elif key == "3":
                row = self._prompt_clean_row()
                if row is not None:
                    self.append_output(
                        f"─── Clean (start row {row}) ───────────────────────"
                    )
                    self.run_script("clean.py", [str(row)])
                    self.append_output("─── Clean complete ────────────────────────────────")
                    self.completed_steps.add(3)

            elif key == "4":
                self.append_output("─── GetWeather ────────────────────────────────────")
                self.run_script("getweather.py")
                self.append_output("─── GetWeather complete ───────────────────────────")
                self.completed_steps.add(4)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Initialise the curses environment and run the launcher."""
    try:
        curses.wrapper(lambda stdscr: Launcher(stdscr).run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
