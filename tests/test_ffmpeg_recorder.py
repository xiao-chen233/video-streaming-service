from app.services.ffmpeg_recorder import FFmpegRecorder


def _build(url: str) -> list[str]:
    recorder = FFmpegRecorder(
        stream_id="cam-1",
        stream_url=url,
        output_dir="/tmp/recordings/cam-1",
        ffmpeg_path="ffmpeg",
        segment_seconds=3600,
    )
    return recorder._build_command()


def test_build_command_rtsp_defaults_mp4() -> None:
    command = _build("rtsp://example/live")
    assert "-rtsp_transport" in command
    assert "tcp" in command
    assert "-segment_format" in command
    assert command[command.index("-segment_format") + 1] == "mp4"
    assert "-movflags" in command
    assert command[-1].endswith("%Y%m%d_%H%M%S.mp4")


def test_build_command_rtmp_defaults_flv() -> None:
    command = _build("rtmp://example/live")
    assert "-rtsp_transport" not in command
    assert "-segment_format" in command
    assert command[command.index("-segment_format") + 1] == "flv"
    assert "-movflags" not in command
    assert command[-1].endswith("%Y%m%d_%H%M%S.flv")
