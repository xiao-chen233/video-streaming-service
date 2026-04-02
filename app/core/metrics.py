from prometheus_client import Counter, Gauge, Histogram


recording_active_streams = Gauge("recording_active_streams", "Number of active recording streams")
recording_restart_count = Counter("recording_restart_count", "Restart count of stream recorders", ["stream_id"])
recording_errors_total = Counter("recording_errors_total", "Total stream errors", ["stream_id", "reason"])
recording_file_write_latency = Histogram(
    "recording_file_write_latency",
    "Latency of file indexing and write metadata",
)
