import re
from urllib.parse import urljoin

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from playwright.sync_api import sync_playwright

app = FastAPI(title="Fair Scraper API", version="1.0.0")

class ScrapeRequest(BaseModel):
    url: str
    countries: list[str] = Field(default_factory=lambda: ["Spain", "Portugal"])
    deep_profile: bool = True

def clean_text(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").strip())

@app.post("/scrape")
def scrape(req: ScrapeRequest):
    if not req.url.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="Invalid url")

    exhibitors = []
    errors = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1400, "height": 900})

        try:
            page.goto(req.url, wait_until="networkidle", timeout=60000)
        except Exception as e:
            browser.close()
            raise HTTPException(status_code=500, detail=f"Navigation failed: {e}")

        base = req.url
        links = page.locator("a")
        count = min(links.count(), 3000)

        for i in range(count):
            try:
                link = links.nth(i)
                text = clean_text(link.inner_text() or "")
                href = link.get_attribute("href") or ""

                if not href or len(text) < 2:
                    continue

                abs_href = urljoin(base, href)

                exhibitors.append({
                    "company_name": text[:200],
                    "country": "",
                    "profile_url": abs_href,
                    "description": "",
                    "raw_text": text[:500],
                })

                if len(exhibitors) >= 2000:
                    break

            except Exception:
                continue

        browser.close()

    seen = set()
    deduped = []

    for x in exhibitors:
        key = (x["company_name"].lower().strip(), x["profile_url"].lower().strip())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(x)

    return {
        "source_url": req.url,
        "exhibitors": deduped,
        "errors": errors
    }
