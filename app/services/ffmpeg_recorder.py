from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import os
import subprocess
import threading
import time


logger = logging.getLogger(__name__)


@dataclass
class FFmpegRecorder:
    stream_id: str
    stream_url: str
    output_dir: str
    ffmpeg_path: str
    segment_seconds: int
    process: subprocess.Popen[str] | None = None
    last_output_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_error: str | None = None
    _stderr_thread: threading.Thread | None = None
    _stop_event: threading.Event = field(default_factory=threading.Event)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def _is_rtsp(self) -> bool:
        return self.stream_url.lower().startswith(("rtsp://", "rtsps://"))

    def _output_container(self) -> str:
        # RTMP family defaults to FLV container; other protocols keep MP4 behavior.
        if self.stream_url.lower().startswith(("rtmp://", "rtmps://")):
            return "flv"
        return "mp4"

    def _build_command(self) -> list[str]:
        os.makedirs(self.output_dir, exist_ok=True)
        output_container = self._output_container()
        output_pattern = os.path.join(self.output_dir, f"%Y%m%d_%H%M%S.{output_container}")
        command = [
            self.ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "info",
        ]
        if self._is_rtsp():
            command.extend(["-rtsp_transport", "tcp"])
        command.extend([
            "-i",
            self.stream_url,
            "-c",
            "copy",
            "-f",
            "segment",
            "-segment_format",
            output_container,
            "-segment_time",
            str(self.segment_seconds),
            "-segment_atclocktime",
            "1",
            "-strftime",
            "1",
            "-reset_timestamps",
            "1",
        ])
        if output_container == "mp4":
            # faststart is MP4-specific and should not be applied to FLV muxing.
            command.extend(["-movflags", "+faststart"])
        command.extend([
            "-reconnect",
            "1",
            "-reconnect_streamed",
            "1",
            "-reconnect_delay_max",
            "2",
            output_pattern,
        ])
        return command

    def start(self, startup_probe_seconds: int = 0) -> None:
        with self._lock:
            if self.process and self.process.poll() is None:
                return
            self._stop_event.clear()
            command = self._build_command()
            logger.info(
                "starting ffmpeg",
                extra={"stream_id": self.stream_id, "event": "start", "status": "STARTING"},
            )
            self.process = subprocess.Popen(
                command,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
            self.last_output_at = datetime.now(timezone.utc)
            self._stderr_thread = threading.Thread(target=self._consume_stderr, daemon=True)
            self._stderr_thread.start()
            if startup_probe_seconds > 0:
                try:
                    deadline = time.time() + startup_probe_seconds
                    while time.time() < deadline:
                        if self.process is None or self.process.poll() is not None:
                            err = self.last_error or "ffmpeg exited during startup probe"
                            raise RuntimeError(err)
                        time.sleep(0.2)
                except Exception:
                    if self.process and self.process.poll() is None:
                        try:
                            self.process.terminate()
                            self.process.wait(timeout=2)
                        except Exception:
                            try:
                                self.process.kill()
                            except Exception:
                                pass
                    if self.process and self.process.stderr:
                        try:
                            self.process.stderr.close()
                        except Exception:
                            pass
                    self.process = None
                    raise

    def _consume_stderr(self) -> None:
        assert self.process is not None
        stderr = self.process.stderr
        if stderr is None:
            return
        for line in stderr:
            if self._stop_event.is_set():
                break
            self.last_output_at = datetime.now(timezone.utc)
            txt = line.strip()
            if not txt:
                continue
            lower_txt = txt.lower()
            if "error" in lower_txt or "failed" in lower_txt or "timed out" in lower_txt:
                self.last_error = txt
                logger.warning(
                    "ffmpeg stderr error",
                    extra={"stream_id": self.stream_id, "event": "stderr", "reason": txt},
                )
        stderr.close()

    def stop(self, kill_timeout_seconds: int = 8) -> None:
        with self._lock:
            self._stop_event.set()
            if not self.process:
                return
            if self.process.poll() is None:
                self.process.terminate()
                deadline = time.time() + kill_timeout_seconds
                while time.time() < deadline and self.process.poll() is None:
                    time.sleep(0.2)
                if self.process.poll() is None:
                    self.process.kill()
            if self.process.stderr:
                try:
                    self.process.stderr.close()
                except Exception:
                    pass
            self.process = None
            logger.info("ffmpeg stopped", extra={"stream_id": self.stream_id, "event": "stop", "status": "STOPPED"})

    def is_alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def has_output_timeout(self, timeout_seconds: int) -> bool:
        if not self.is_alive():
            return True
        elapsed = datetime.now(timezone.utc) - self.last_output_at
        return elapsed.total_seconds() > timeout_seconds
