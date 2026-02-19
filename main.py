import re
from urllib.parse import urljoin, urlparse, urlencode, parse_qs

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

app = FastAPI(title="Fair Scraper API", version="1.1.0")


class ScrapeRequest(BaseModel):
    url: str
    countries: list[str] = Field(default_factory=lambda: ["Spain", "Portugal"])
    deep_profile: bool = True
    max_pages: int = 15
    max_exhibitors: int = 400


def clean_text(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").strip())


def normalize_country_from_text(text: str) -> str:
    t = (text or "").lower()
    # señales típicas (ES/PT) en descripciones
    if "españa" in t or "espana" in t or "spain" in t:
        return "Spain"
    if "portugal" in t:
        return "Portugal"
    # códigos
    if re.search(r"\bES\b", text or ""):
        return "Spain"
    if re.search(r"\bPT\b", text or ""):
        return "Portugal"
    return ""


def want_country(country: str, allow: list[str]) -> bool:
    if not allow:
        return True
    allow_norm = set([a.strip().lower() for a in allow])
    return country.strip().lower() in allow_norm


def set_query_param(url: str, key: str, value: str) -> str:
    u = urlparse(url)
    qs = parse_qs(u.query)
    qs[key] = [value]
    new_q = urlencode(qs, doseq=True)
    return u._replace(query=new_q).geturl()


EXHIBITOR_PATH_RE = re.compile(r"/es/exhibitors/[^/?#]+-\d+")


def is_exhibitor_profile(href_abs: str) -> bool:
    try:
        path = urlparse(href_abs).path
    except Exception:
        return False
    return bool(EXHIBITOR_PATH_RE.search(path))


def dedupe(items: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for it in items:
        key = (it.get("company_name", "").strip().lower(), it.get("profile_url", "").strip().lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def extract_profiles_from_listing(page, base_url: str) -> list[dict]:
    results = []
    links = page.locator("a")
    n = min(links.count(), 5000)
    for i in range(n):
        try:
            loc = links.nth(i)
            href = loc.get_attribute("href") or ""
            if not href:
                continue
            href_abs = urljoin(base_url, href)
            if not is_exhibitor_profile(href_abs):
                continue

            name = clean_text(loc.inner_text() or "")
            # si el anchor no tiene texto, ignoramos
            if len(name) < 2:
                continue

            results.append(
                {
                    "company_name": name[:200],
                    "country": "",
                    "profile_url": href_abs,
                    "description": "",
                    "raw_text": "",
                }
            )
        except Exception:
            continue
    return dedupe(results)


def enrich_profile(context, profile_url: str) -> tuple[str, str, str]:
    """
    Devuelve: (country, description, raw_text)
    """
    page = context.new_page(viewport={"width": 1400, "height": 900})
    try:
        page.goto(profile_url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(500)

        body = clean_text(page.locator("body").inner_text() or "")
        # description: intenta meta description, si no, primer bloque de texto razonable
        desc = ""
        try:
            meta = page.locator("meta[name='description']")
            if meta.count() > 0:
                desc = clean_text(meta.first.get_attribute("content") or "")
        except Exception:
            pass
        if not desc:
            # toma un chunk del body
            desc = body[:400]

        country = normalize_country_from_text(body)

        return country, desc[:400], body[:3000]
    except PWTimeoutError:
        return "", "", ""
    except Exception:
        return "", "", ""
    finally:
        try:
            page.close()
        except Exception:
            pass


@app.post("/scrape")
def scrape(req: ScrapeRequest):
    if not req.url.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="Invalid url")

    errors: list[str] = []
    exhibitors: list[dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()

        # Iterar páginas del catálogo (stands[page]=1..N)
        for page_num in range(1, req.max_pages + 1):
            listing_url = set_query_param(req.url, "stands[page]", str(page_num))

            page = context.new_page(viewport={"width": 1400, "height": 900})
            try:
                page.goto(listing_url, wait_until="networkidle", timeout=60000)
                page.wait_for_timeout(700)
            except Exception as e:
                errors.append(f"Listing page {page_num} failed: {e}")
                try:
                    page.close()
                except Exception:
                    pass
                break

            found = extract_profiles_from_listing(page, req.url)
            exhibitors.extend(found)
            exhibitors = dedupe(exhibitors)

            try:
                page.close()
            except Exception:
                pass

            if len(exhibitors) >= req.max_exhibitors:
                exhibitors = exhibitors[: req.max_exhibitors]
                break

            # Heurística: si una página no añade nuevos, parar
            if page_num >= 2 and len(found) == 0:
                break

        # Enriquecer fichas
        if req.deep_profile:
            enriched = []
            for it in exhibitors[: req.max_exhibitors]:
                c, desc, raw = enrich_profile(context, it["profile_url"])
                it["country"] = c
                it["description"] = desc
                it["raw_text"] = raw
                enriched.append(it)
            exhibitors = enriched

        # Filtrar ES/PT si viene country
        allow = [a.strip().lower() for a in req.countries]
        filtered = []
        for it in exhibitors:
            c = (it.get("country") or "").strip()
            if not c:
                # si no hay país, lo dejamos pasar (tu GPT puede marcar No confirmado)
                filtered.append(it)
            else:
                if want_country(c, allow):
                    filtered.append(it)

        browser.close()

    return {
        "source_url": req.url,
        "exhibitors": filtered,
        "errors": errors,
    }
