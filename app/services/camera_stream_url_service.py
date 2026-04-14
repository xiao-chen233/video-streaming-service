from __future__ import annotations

import json
import re
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
from urllib.request import Request, urlopen


class CameraStreamUrlService:
    def __init__(self, api_url: str, protocol: str = "rtmp", timeout_seconds: int = 6):
        self.api_url = api_url
        self.protocol = protocol
        self.timeout_seconds = timeout_seconds

    def fetch_temporary_url(self, camera_gb_code: str) -> str:
        request_url = self._build_request_url(camera_gb_code)
        req = Request(request_url, method="GET")
        with urlopen(req, timeout=self.timeout_seconds) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            content_type = resp.headers.get("Content-Type", "")

        stream_url = self._extract_stream_url(body=body, content_type=content_type)
        if not stream_url:
            raise RuntimeError(f"temporary stream url is empty for camera_gb_code={camera_gb_code}")
        return stream_url

    def _build_request_url(self, camera_gb_code: str) -> str:
        parsed = urlparse(self.api_url)
        existing_query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        existing_query["protocol"] = self.protocol
        existing_query["gbIndexCode"] = camera_gb_code
        new_query = urlencode(existing_query)
        return urlunparse(parsed._replace(query=new_query))

    def _extract_stream_url(self, body: str, content_type: str) -> str | None:
        expected_prefix = f"{self.protocol.lower()}://"

        def find_in_obj(obj: object) -> str | None:
            if isinstance(obj, str):
                candidate = self._extract_from_text(obj, expected_prefix)
                return candidate
            if isinstance(obj, dict):
                for value in obj.values():
                    candidate = find_in_obj(value)
                    if candidate:
                        return candidate
                return None
            if isinstance(obj, list):
                for value in obj:
                    candidate = find_in_obj(value)
                    if candidate:
                        return candidate
                return None
            return None

        if "json" in content_type.lower() or body.strip().startswith("{") or body.strip().startswith("["):
            try:
                parsed = json.loads(body)
                candidate = find_in_obj(parsed)
                if candidate:
                    return candidate
            except json.JSONDecodeError:
                pass

        return self._extract_from_text(body, expected_prefix)

    @staticmethod
    def _extract_from_text(text: str, expected_prefix: str) -> str | None:
        stripped = text.strip().strip('"').strip("'")
        if stripped.lower().startswith(expected_prefix):
            return stripped

        escaped = re.escape(expected_prefix)
        pattern = re.compile(rf"({escaped}[^\s'\"<>]+)", re.IGNORECASE)
        match = pattern.search(text)
        if match:
            return match.group(1)
        return None
