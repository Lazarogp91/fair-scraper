from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from playwright.async_api import async_playwright
import re

app = FastAPI(title="Fair Scraper API")

class ScrapeRequest(BaseModel):
    url: str
    countries: List[str] = ["Spain", "Portugal"]
    manufacturers_only: bool = True
    max_pages: int = 3

@app.post("/scrape")
async def scrape_fair(request: ScrapeRequest):
    try:
        results = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(request.url, timeout=60000)
            await page.wait_for_timeout(5000)

            # Detectar tipo Easyfairs
            content = await page.content()

            if "algolia" in content.lower():
                # Buscar llamadas fetch a Algolia
                requests_data = []

                async def intercept(route):
                    request_obj = route.request
                    if "algolia" in request_obj.url:
                        requests_data.append(request_obj.url)
                    await route.continue_()

                await page.route("**/*", intercept)
                await page.reload()
                await page.wait_for_timeout(5000)

            # Extraer tarjetas visibles
            cards = await page.query_selector_all("article, .card, .stand, .exhibitor")

            for card in cards:
                text = await card.inner_text()
                text_lower = text.lower()

                # Filtro país
                if not any(c.lower() in text_lower for c in request.countries):
                    continue

                # Filtro fabricante
                if request.manufacturers_only:
                    keywords = [
                        "fabricante",
                        "manufactur",
                        "diseña",
                        "design",
                        "produce",
                        "manufactures"
                    ]
                    if not any(k in text_lower for k in keywords):
                        continue

                results.append(text.strip())

            await browser.close()

        return {
            "status": "ok",
            "total_detected": len(results),
            "results": results
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
