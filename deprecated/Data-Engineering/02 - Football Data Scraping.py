# Databricks notebook source
# MAGIC %md
# MAGIC # Football Data Scraping

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog football;
# MAGIC use catalog football;
# MAGIC create schema demo;
# MAGIC use schema demo;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Install playwright and beautifulsoup4
# MAGIC %pip install playwright beautifulsoup4

# COMMAND ----------

# DBTITLE 1,Install Playwright Chromium browser
!playwright install chromium

# COMMAND ----------

# DBTITLE 1,Cell 4
"""
Scrape La Liga schedule/results from FBRef and write to a Delta Table.
Handles merged cells (Wk, Day, Date) and extracts all match rows.
"""

import re
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


BASE_URL_TEMPLATE = "https://fbref.com/en/comps/12/{season}/schedule/{season}-La-Liga-Scores-and-Fixtures"
DEFAULT_SEASON = "2023-2024"  # previous season
DELTA_TABLE_NAME = "football.demo.la_liga_schedule"
# Delay between seasons when using --all-seasons (reduces Cloudflare rate limiting)
DELAY_BETWEEN_SEASONS_SEC = 10

# All La Liga seasons used as input when running over multiple seasons (e.g. --all-seasons).
LA_LIGA_SEASONS = [
    "2000-2001",
    "2001-2002",
    "2002-2003",
    "2003-2004",
    "2004-2005",
    "2005-2006",
    "2006-2007",
    "2007-2008",
    "2008-2009",
    "2009-2010",
    "2010-2011",
    "2011-2012",
    "2012-2013",
    "2013-2014",
    "2014-2015",
    "2015-2016",
    "2016-2017",
    "2017-2018",
    "2018-2019",
    "2019-2020",
    "2020-2021",
    "2021-2022",
    "2022-2023",
    "2023-2024",
    "2024-2025",
]


def season_from_url(url: str) -> str | None:
    """Extract season (e.g. 2023-2024) from FBRef schedule URL if possible."""
    m = re.search(r"comps/12/(\d{4}-\d{4})/schedule", url)
    return m.group(1) if m else None


def url_for_season(season: str) -> str:
    """Build FBRef La Liga schedule URL for a given season."""
    return BASE_URL_TEMPLATE.format(season=season)


def extract_text(cell):
    """Get link text or cell text, stripped."""
    a = cell.find("a")
    return (a.get_text(strip=True) if a else cell.get_text(strip=True)) or ""


def parse_time(raw: str) -> str:
    """Use primary time only; e.g. '20:30 (21:30)' -> '20:30'."""
    if not raw:
        return ""
    return raw.split("(")[0].strip()


def parse_score(raw: str) -> tuple[int | None, int | None]:
    """Parse '1-1' -> (1, 1). Returns (None, None) for unplayed."""
    if not raw or "-" not in raw:
        return None, None
    parts = raw.strip().split("-")
    if len(parts) != 2:
        return None, None
    try:
        return int(parts[0].strip()), int(parts[1].strip())
    except ValueError:
        return None, None


def parse_attendance(raw: str) -> int | None:
    """Parse '47,845' -> 47845."""
    if not raw:
        return None
    try:
        return int(raw.replace(",", "").strip())
    except ValueError:
        return None


def parse_date(raw: str) -> str:
    """Normalize date to YYYY-MM-DD if possible."""
    if not raw:
        return ""
    raw = raw.strip()
    if re.match(r"\d{4}-\d{2}-\d{2}", raw):
        return raw
    return raw


def _is_cloudflare_challenge(html: str) -> bool:
    """True if the page is still showing Cloudflare challenge (e.g. 'Just a moment', Turnstile)."""
    if not html:
        return True
    lower = html.lower()
    return (
        "just a moment" in lower
        or "verify you are human" in lower
        or "challenge-platform" in lower
        or "cf-chl-" in lower
        or "turnstile" in lower
    )


def get_page_html(
    url: str,
    headless: bool = True,
    max_retries: int = 3,
    wait_after_load_sec: float = 5.0,
    delay_between_retries_sec: float = 15.0,
) -> str:
    """Load page with Playwright. More robust to Cloudflare: stealth args, retries, long wait for Turnstile."""
    chromium_args = [
        "--disable-blink-features=AutomationControlled",
        "--disable-features=ImprovedCookieControls",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-infobars",
        "--window-size=1280,720",
    ]
    user_agent = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    for attempt in range(max_retries):
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(
                    channel="chrome",
                    headless=headless,
                    args=chromium_args,
                )
            except Exception:
                try:
                    browser = p.chromium.launch(
                        headless=headless,
                        args=chromium_args,
                    )
                except Exception:
                    browser = p.chromium.launch(headless=headless)
            context = browser.new_context(
                user_agent=user_agent,
                viewport={"width": 1280, "height": 720},
                java_script_enabled=True,
                locale="en-US",
                timezone_id="Europe/Madrid",
                ignore_https_errors=False,
            )
            # Reduce automation signals
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            )
            page = context.new_page()
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                # Allow Cloudflare / Turnstile to run
                page.wait_for_timeout(int(wait_after_load_sec * 1000))
                # Wait for schedule table (indicates challenge passed)
                try:
                    page.wait_for_selector(
                        "table[id^='sched'], table.stats_table",
                        timeout=90000,
                    )
                except Exception:
                    pass
                page.wait_for_timeout(3000)
                html = page.content()
            finally:
                browser.close()
        if not _is_cloudflare_challenge(html):
            return html
        if attempt < max_retries - 1:
            import time
            time.sleep(delay_between_retries_sec)
    return html


def find_schedule_table(soup: BeautifulSoup):
    """Find the main schedule/fixtures table (Wk, Day, Date, Time, Home, Score, Away, ...)."""
    for table in soup.find_all("table"):
        header_row = table.find("thead")
        if not header_row:
            continue
        headers = [th.get_text(strip=True).lower() for th in header_row.find_all("th")]
        if "wk" in headers and "date" in headers and "home" in headers and "score" in headers:
            return table
    return None


def scrape_schedule_table(table) -> list[dict]:
    """
    Parse schedule table handling rowspan for Wk, Day, Date.
    Returns list of dicts, one per match row.
    """
    thead = table.find("thead")
    header_cells = thead.find_all("th") if thead else []
    col_names = [th.get_text(strip=True).lower() for th in header_cells]
    def find_col(name: str, alt: str | None = None) -> int:
        try:
            return col_names.index(name)
        except ValueError:
            if alt:
                return col_names.index(alt)
            raise

    try:
        wk_idx = find_col("wk")
        day_idx = find_col("day")
        date_idx = find_col("date")
        time_idx = find_col("time")
        home_idx = find_col("home")
        score_idx = find_col("score")
        away_idx = find_col("away")
        att_idx = find_col("attendance", "att")
        venue_idx = find_col("venue")
        ref_idx = find_col("referee")
    except ValueError:
        return []

    tbody = table.find("tbody")
    body_rows = tbody.find_all("tr") if tbody else []
    num_cols = len(col_names)

    # Per-column rowspan: (value, rows_remaining). When remaining > 0, no cell in that column for next row.
    rowspan_state = [None] * num_cols  # each is (value, remaining) or None

    matches = []
    for tr in body_rows:
        cells = tr.find_all(["th", "td"])
        values = [None] * num_cols
        cell_iter = iter(cells)
        for col in range(num_cols):
            if rowspan_state[col] is not None:
                val, remaining = rowspan_state[col]
                values[col] = val
                if remaining <= 1:
                    rowspan_state[col] = None
                else:
                    rowspan_state[col] = (val, remaining - 1)
                continue
            try:
                c = next(cell_iter)
            except StopIteration:
                break
            text = extract_text(c)
            values[col] = text
            rowspan = int(c.get("rowspan", 1))
            if rowspan > 1:
                rowspan_state[col] = (text, rowspan - 1)

        def v(i, default=""):
            if i < len(values) and values[i] is not None:
                return values[i]
            return default

        home = v(home_idx)
        if not home:
            continue

        score_raw = v(score_idx)
        home_goals, away_goals = parse_score(score_raw)

        wk_val = v(wk_idx)
        matches.append({
            "wk": int(wk_val) if wk_val.isdigit() else None,
            "day": v(day_idx),
            "date": parse_date(v(date_idx)),
            "time": parse_time(v(time_idx)),
            "home": home,
            "score": score_raw,
            "home_goals": home_goals,
            "away_goals": away_goals,
            "away": v(away_idx),
            "attendance": parse_attendance(v(att_idx)),
            "venue": v(venue_idx),
            "referee": v(ref_idx),
            # "season" is set in main() from URL or --season
        })

    return matches


def unpack_commented_tables(soup: BeautifulSoup) -> None:
    """FBRef sometimes embeds tables in HTML comments; unpack them into the tree."""
    from bs4 import Comment
    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        comment_str = comment.extract()
        if "table" not in comment_str:
            continue
        try:
            frag = BeautifulSoup(comment_str, "html.parser")
            table = frag.find("table")
            if table:
                comment.insert_before(table)
        except Exception:
            pass


def parse_args():
    import sys
    argv = sys.argv[1:]
    args = [a for a in argv if not a.startswith("--")]
    html_file = Path(args[0]) if args and Path(args[0]).exists() else None
    headed = "--headed" in argv
    all_seasons = "--all-seasons" in argv
    season = None
    url = None
    i = 0
    while i < len(argv):
        if argv[i] == "--season" and i + 1 < len(argv):
            season = argv[i + 1]
            i += 2
            continue
        if argv[i] == "--url" and i + 1 < len(argv):
            url = argv[i + 1]
            i += 2
            continue
        i += 1
    return html_file, headed, all_seasons, season, url


def scrape_season(html: str, season: str) -> pd.DataFrame | None:
    """Parse HTML, extract schedule table, return DataFrame with season column; None if no table/matches."""
    soup = BeautifulSoup(html, "html.parser")
    unpack_commented_tables(soup)
    table = find_schedule_table(soup)
    if not table:
        return None
    matches = scrape_schedule_table(table)
    if not matches:
        return None
    df = pd.DataFrame(matches)
    df.insert(0, "season", season)
    return df


def main():
    import sys
    html_file, headed, all_seasons, season_arg, url_arg = parse_args()

    if all_seasons and not html_file:
        import time
        seasons_to_run = LA_LIGA_SEASONS
        print(f"Running for {len(seasons_to_run)} seasons (delay {DELAY_BETWEEN_SEASONS_SEC}s between requests to reduce Cloudflare blocks).")
        frames = []
        for i, season in enumerate(seasons_to_run):
            if i > 0:
                time.sleep(DELAY_BETWEEN_SEASONS_SEC)
            url_used = url_for_season(season)
            print(f"Fetching {url_used} (season={season})...")
            html = None
            for try_fetch in range(2):  # retry once per season if we got Cloudflare page
                try:
                    html = get_page_html(
                        url_used,
                        headless=not headed,
                        wait_after_load_sec=8.0 if try_fetch == 0 else 20.0,
                    )
                except Exception as e:
                    print(f"  Fetch error: {e}")
                    break
                if html and not _is_cloudflare_challenge(html):
                    break
                if try_fetch == 0:
                    print(f"  Got Cloudflare challenge, retrying with longer wait...")
                    time.sleep(15)
            if not html or _is_cloudflare_challenge(html):
                print(f"  Skip {season}: Cloudflare block or fetch failed.")
                continue
            df = scrape_season(html, season)
            if df is not None and len(df) > 0:
                frames.append(df)
                print(f"  Scraped {len(df)} matches.")
            else:
                print(f"  No schedule table found for {season}.")
        if not frames:
            print("No data scraped for any season.")
            return
        df = pd.concat(frames, ignore_index=True)
        spark_df = spark.createDataFrame(df)
        spark_df.write.format("delta").mode("overwrite").saveAsTable(DELTA_TABLE_NAME)
        print(f"Wrote {len(df)} total matches to {DELTA_TABLE_NAME}")
        print(df[["season", "date", "home", "score", "away"]].head(5).to_string())
        return

    if html_file:
        print(f"Loading HTML from {html_file}...")
        html = html_file.read_text(encoding="utf-8")
        season = season_arg or DEFAULT_SEASON
        url_used = None
    else:
        import time
        url_used = url_arg or url_for_season(season_arg or DEFAULT_SEASON)
        season = season_arg or season_from_url(url_used) or DEFAULT_SEASON
        print(f"Fetching {url_used} (season={season})...")
        print("(FBRef may show Cloudflare; use --headed or save HTML and pass file.)")
        html = None
        for try_fetch in range(2):
            try:
                html = get_page_html(
                    url_used,
                    headless=not headed,
                    wait_after_load_sec=8.0 if try_fetch == 0 else 25.0,
                )
            except Exception as e:
                print(f"Fetch failed: {e}")
                if try_fetch == 0:
                    print("Retrying once with longer wait...")
                    time.sleep(15)
                    continue
                print("Options: 1) Run with --headed. 2) Save the page as HTML and run: python scrape_fbref.py your_page.html --season 2023-2024")
                raise
            if html and not _is_cloudflare_challenge(html):
                break
            if try_fetch == 0:
                print("Got Cloudflare challenge, retrying with longer wait...")
                time.sleep(15)
        if not html or _is_cloudflare_challenge(html):
            print("Cloudflare block: schedule not available. Saving page as debug_page.html.")
            if html:
                Path("debug_page.html").write_text(html, encoding="utf-8")
            print("Options: 1) Run with --headed. 2) Save the page manually and run: python scrape_fbref.py your_page.html --season 2023-2024")
            return

    df = scrape_season(html, season)
    if df is None:
        print("Schedule table not found. Saving HTML for inspection.")
        Path("debug_page.html").write_text(html, encoding="utf-8")
        return
    if len(df) == 0:
        print("No match rows extracted. Saving HTML as debug_page.html")
        Path("debug_page.html").write_text(html, encoding="utf-8")
        return

    print(f"Scraped {len(df)} matches for season {season}.")
    spark_df = spark.createDataFrame(df)
    spark_df.write.format("delta").mode("overwrite").saveAsTable(DELTA_TABLE_NAME)
    print(f"Wrote Delta Table to: {DELTA_TABLE_NAME}")
    print(df.head(3).to_string())


if __name__ == "__main__":
    main()