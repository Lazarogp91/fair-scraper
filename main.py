from __future__ import annotations

import re
import json
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


# -----------------------------
# Config
# -----------------------------
DEFAULT_COUNTRIES = ["Spain", "Portugal"]

# Keywords for strict manufacturer evidence (ES/EN/PT)
MANUFACTURER_KEYWORDS = [
    # ES
    r"\bfabricante(s)?\b",
    r"\bfabricación\b",
    r"\bproducción\b",
    r"\bplanta\b.*\bproducci[oó]n\b",
    r"\bdiseñ(amos|a|an)\s+y\s+(fabricamos|produce|producimos)\b",
    r"\bOEM\b",
    r"\bmarca\s+propia\b",
    # EN
    r"\bmanufacturer(s)?\b",
    r"\bmanufacturing\b",
    r"\bwe\s+manufacture\b",
    r"\bwe\s+design\s+and\s+(manufacture|produce)\b",
    r"\bin[-\s]?house\s+production\b",
    r"\bown\s+production\b",
    r"\bproduction\s+plant\b",
    # PT
    r"\bfabricante(s)?\b",
    r"\bfabrica[cç][aã]o\b",
    r"\bprodu[cç][aã]o\b",
    r"\bOEM\b",
    r"\bmarca\s+pr[oó]pria\b",
]

NEGATIVE_NON_MANUFACTURER_HINTS = [
    r"\bdistribuidor(a|es)?\b",
    r"\bdistribuci[oó]n\b",
    r"\bintegrador(a|es)?\b",
    r"\bconsultor[ií]a\b",
    r"\bservicios\b",
    r"\bpartner(s)?\b.*\bfabricante\b",  # "partner de fabricantes" -> no implica fabricar
    r"\breseller\b",
]

ALGOLIA_APPID_RE = re.compile(r"\b([A-Z0-9]{8,12})\b")
ALGOLIA_APIKEY_RE = re.compile(r"\b([a-zA-Z0-9]{24,64})\b")
INDEX_RE = re.compile(r"\bstands_\d{8,12}\b")
CONTAINERID_RE = re.compile(r"\bcontainerId\b[^0-9]{0,10}(\d{3,6})\b", re.IGNORECASE)


# -----------------------------
# Models
# -----------------------------
class ScrapeRequest(BaseModel):
    url: str = Field(..., description="URL of the fair exhibitor catalog")
    countries: List[str] = Field(default_factory=lambda: DEFAULT_COUNTRIES)
    manufacturers_only: bool = Field(default=True, description="Return only manufacturers")
    max_pages: int = Field(default=50, ge=1, le=500, description="Max Algolia pages to iterate")
    hits_per_page: int = Field(default=100, ge=10, le=200, description="Algolia hits per page")
    timeout_ms: int = Field(default=30000, ge=5000, le=120000, description="Timeout for network/browser ops")


class ExhibitorOut(BaseModel):
    company: str
    country: Optional[str] = None
    fair_profile_url: Optional[str] = None
    what_they_make: Optional[str] = None
    category: Optional[str] = None
    evidence_manufacturer: str
    confidence: str
    sensors_potential: str


class ScrapeResponse(BaseModel):
    status: str
    mode: str
    total_detected: int
    total_espt: int
    total_manufacturers: int
    results: List[ExhibitorOut]
    debug: Dict[str, Any] = Field(default_factory=dict)


# -----------------------------
# Helpers
# -----------------------------
def normalize_base(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}".rstrip("/")


def pick_text(desc: Any) -> str:
    """
    desc might be dict {en:..., es:...} or string or None
    """
    if desc is None:
        return ""
    if isinstance(desc, str):
        return desc
    if isinstance(desc, dict):
        # prefer ES, then EN, then any
        for k in ("es", "en", "pt"):
            if k in desc and isinstance(desc[k], str) and desc[k].strip():
                return desc[k]
        for v in desc.values():
            if isinstance(v, str) and v.strip():
                return v
    return ""


def pick_category(categories: Any) -> Optional[str]:
    if not categories or not isinstance(categories, list):
        return None
    # categories elements like {"name": {"en": "...", "es": "..."}} or similar
    names
