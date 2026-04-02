from __future__ import annotations

from app.services.ffmpeg_recorder import FFmpegRecorder


class RecordingService:
    """
    Single-stream service wrapper.
    StreamManager composes this capability for multi-stream orchestration.
    """

    def __init__(self, recorder: FFmpegRecorder):
        self.recorder = recorder

    def start_stream(self) -> None:
        self.recorder.start()

    def stop_stream(self) -> None:
        self.recorder.stop()

    def is_alive(self) -> bool:
        return self.recorder.is_alive()
