"""Record a four-terminal video from real server/client subprocess output.

Optional tooling (not required to run the Go application): Python, Pillow,
and ffmpeg on PATH. Run `python demo/record.py` from any directory.
"""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import textwrap
import threading
import time

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "demo" / "chat-demo.mp4"
FPS = 8
DURATION = 29
WIDTH, HEIGHT = 1600, 900


class Terminal:
    def __init__(self, label: str) -> None:
        self.label = label
        self.text = "Waiting to connect...\n" if label != "SERVER" else ""
        self.lock = threading.Lock()
        self.process: subprocess.Popen[str] | None = None

    def append(self, text: str) -> None:
        with self.lock:
            self.text += text

    def snapshot(self) -> str:
        with self.lock:
            return self.text


def capture_output(terminal: Terminal) -> None:
    assert terminal.process is not None and terminal.process.stdout is not None
    while char := terminal.process.stdout.read(1):
        terminal.append(char)


def start(terminal: Terminal, command: list[str], *, input_enabled: bool) -> None:
    terminal.text = ""
    terminal.process = subprocess.Popen(
        command,
        cwd=ROOT,
        stdin=subprocess.PIPE if input_enabled else subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    threading.Thread(target=capture_output, args=(terminal,), daemon=True).start()


def await_output(terminal: Terminal, expected: str, timeout: float = 4) -> None:
    deadline = time.monotonic() + timeout
    while expected not in terminal.snapshot():
        if time.monotonic() >= deadline:
            raise RuntimeError(f"{terminal.label} did not print {expected!r}: {terminal.snapshot()!r}")
        time.sleep(0.04)


def send(terminal: Terminal, text: str) -> None:
    assert terminal.process is not None and terminal.process.stdin is not None
    await_output(terminal, "Connected as")
    terminal.append(text + "\n")  # A real terminal echoes the user's input.
    terminal.process.stdin.write(text + "\n")
    terminal.process.stdin.flush()


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def font(size: int, *, bold: bool = False, mono: bool = False) -> ImageFont.FreeTypeFont:
    if os.name == "nt":
        candidate = Path("C:/Windows/Fonts") / (
            "consola.ttf" if mono else "segoeuib.ttf" if bold else "segoeui.ttf"
        )
        if candidate.exists():
            return ImageFont.truetype(str(candidate), size)
    return ImageFont.truetype("DejaVuSansMono.ttf" if mono else "DejaVuSans.ttf", size)


TITLE_FONT = font(28, bold=True)
SMALL_FONT = font(16)
MONO_FONT = font(17, mono=True)
PANE_FONT = font(19, bold=True)


def frame(terminals: list[Terminal], elapsed: float) -> bytes:
    image = Image.new("RGB", (WIDTH, HEIGHT), "#0b1020")
    draw = ImageDraw.Draw(image)
    draw.text((24, 17), "GO RPC CHAT ROOM", font=TITLE_FONT, fill="#e2e8f0")
    draw.text(
        (370, 27),
        "Real Go processes  |  net/rpc over TCP  |  polling every 500 ms",
        font=SMALL_FONT,
        fill="#94a3b8",
    )
    locations = ((20, 74), (810, 74), (20, 477), (810, 477))
    for terminal, (x, y) in zip(terminals, locations):
        right, bottom = x + 770, y + 390
        draw.rounded_rectangle((x, y, right, bottom), radius=12, fill="#111827", outline="#334155", width=2)
        draw.rounded_rectangle((x + 2, y + 2, right - 2, y + 42), radius=10, fill="#1e293b")
        draw.rectangle((x + 2, y + 32, right - 2, y + 43), fill="#1e293b")
        running = terminal.process is not None and terminal.process.poll() is None
        draw.ellipse((x + 18, y + 16, x + 28, y + 26), fill="#22c55e" if running else "#64748b")
        draw.text((x + 43, y + 10), terminal.label, font=PANE_FONT, fill="#f8fafc")

        raw = terminal.snapshot().replace("\r", "")
        lines: list[str] = []
        for line in raw.split("\n"):
            lines.extend(textwrap.wrap(line, width=76, replace_whitespace=False, drop_whitespace=False) or [""])
        for i, line in enumerate(lines[-14:]):
            color = "#d1d5db"
            if " joined the room" in line:
                color = "#4ade80"
            elif " left the room" in line or "connection lost" in line:
                color = "#fbbf24"
            elif line.startswith("[me]"):
                color = "#60a5fa"
            draw.text((x + 14, y + 57 + i * 22), line, font=MONO_FONT, fill=color)

    draw.text(
        (22, 878),
        "Recorded from live program output and scripted terminal input; no mocked chat events.",
        font=SMALL_FONT,
        fill="#94a3b8",
    )
    draw.text((1513, 878), f"{elapsed:04.1f}s", font=SMALL_FONT, fill="#94a3b8")
    return image.tobytes()


def record(server_exe: Path, client_exe: Path) -> None:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise RuntimeError("ffmpeg is required to encode the demo video")
    port = free_port()
    address = f"127.0.0.1:{port}"
    server, alice, bob, carol = [Terminal(label) for label in ("SERVER", "ALICE", "BOB", "CAROL")]
    terminals = [server, alice, bob, carol]
    encoder: subprocess.Popen[bytes] | None = None
    try:
        start(server, [str(server_exe), "-addr", address], input_enabled=False)
        await_output(server, "listening on")

        def connect(terminal: Terminal) -> None:
            start(terminal, [str(client_exe), "-name", terminal.label.lower(), "-addr", address], input_enabled=True)
            await_output(terminal, "Connected as")

        actions = [
            (1, lambda: connect(alice)),
            (3, lambda: connect(bob)),
            (5, lambda: send(alice, "Hello Bob!")),
            (7, lambda: connect(carol)),
            (9, lambda: send(carol, "/users")),
            (11, lambda: send(bob, "Hi Alice and Carol!")),
            (13, lambda: send(carol, "Nice to meet you both.")),
            (15, lambda: send(alice, "Welcome to the chat room.")),
            (17, lambda: send(bob, "/quit")),
            (19, lambda: send(carol, "Bob has left, but we are still here.")),
            (21, lambda: carol.process.kill()),  # Simulate an unexpected disconnect.
            (23, lambda: send(alice, "/users")),
            (26, lambda: send(alice, "/quit")),
        ]

        encoder = subprocess.Popen(
            [ffmpeg, "-hide_banner", "-loglevel", "error", "-y", "-f", "rawvideo",
             "-pixel_format", "rgb24", "-video_size", f"{WIDTH}x{HEIGHT}", "-framerate", str(FPS),
             "-i", "-", "-an", "-c:v", "libx264", "-preset", "veryfast", "-crf", "26",
             "-pix_fmt", "yuv420p", "-movflags", "+faststart", str(OUTPUT)],
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert encoder.stdin is not None
        began = time.monotonic()
        next_action = 0
        for number in range(DURATION * FPS):
            target = began + number / FPS
            if target > time.monotonic():
                time.sleep(target - time.monotonic())
            elapsed = time.monotonic() - began
            while next_action < len(actions) and actions[next_action][0] <= elapsed:
                actions[next_action][1]()
                next_action += 1
            encoder.stdin.write(frame(terminals, elapsed))

        encoder.stdin.close()
        errors = encoder.stderr.read().decode("utf-8", errors="replace") if encoder.stderr else ""
        if encoder.wait() != 0:
            raise RuntimeError(f"ffmpeg failed: {errors}")
    finally:
        if encoder is not None and encoder.poll() is None:
            encoder.kill()
            encoder.wait()
        for terminal in terminals:
            process = terminal.process
            if process is not None and process.poll() is None:
                process.kill()
                process.wait()
    print(f"Saved real-session demo to {OUTPUT}")


def main() -> None:
    go = shutil.which("go")
    if not go:
        raise RuntimeError("Install Go and add it to PATH before recording")
    with tempfile.TemporaryDirectory(prefix="chat-room-demo-") as folder:
        suffix = ".exe" if os.name == "nt" else ""
        server = Path(folder) / ("server" + suffix)
        client = Path(folder) / ("client" + suffix)
        subprocess.run([go, "build", "-o", str(server), "./cmd/server"], cwd=ROOT, check=True)
        subprocess.run([go, "build", "-o", str(client), "./cmd/client"], cwd=ROOT, check=True)
        record(server, client)


if __name__ == "__main__":
    main()
