from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import HTMLResponse


router = APIRouter(tags=["ui"])

_DASHBOARD_HTML = Path(__file__).with_name("dashboard.html")


@router.get("/", response_class=HTMLResponse)
async def dashboard() -> str:
    return _DASHBOARD_HTML.read_text(encoding="utf-8")
