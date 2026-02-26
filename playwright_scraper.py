from __future__ import annotations

from typing import List, Dict, Any, Tuple
from playwright.sync_api import sync_playwright


def scrape_with_playwright(url: str, timeout_s: int = 30) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.goto(url, timeout=timeout_s * 1000)

        # Espera carga JS
        page.wait_for_timeout(3000)

        html = page.content()

        # Heurística básica: detectar posibles cards
        elements = page.query_selector_all("a")

        seen = set()

        for el in elements:
            text = (el.inner_text() or "").strip()
            if len(text) > 3 and len(text) < 80:
                if text.lower() in seen:
                    continue
                seen.add(text.lower())
                results.append({
                    "fabricante": text,
                    "actividad": "",
                    "enlace_web": "",
                    "pais": ""
                })

        browser.close()

    return results, {
        "driver": "playwright",
        "supported": True,
        "note": "JS dynamic fallback"
    }
