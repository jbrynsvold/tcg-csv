"""
tcgcsv_ingest.py
----------------
Daily ingest of TCGplayer prices via TCGCSV.com into Supabase.

Flow:
  1. Check last-updated.txt — skip if already synced today
  2. SHIFT:  One SQL UPDATE moves all market_d columns forward one day for
             every row in tcgcsv_prices_wide, unconditionally:
               market_d29 <- market_d28 <- ... <- market_d1 <- market_price
             market_price is then set to NULL, ready for today's value.
  3. Load active games from tcgcsv_sync_config
  4. For each game, fetch Groups.csv → upsert tcgcsv_groups
  5. Download each group's ProductsAndPrices.csv (async, 20 concurrent)
  6. Upsert products → tcgcsv_products
  7. Upsert today's 5 price fields → tcgcsv_prices_wide by product_id+sub_type
     New cards get a fresh row. Existing cards get today's prices written on
     top of already-shifted columns. Cards not in feed stay NULL — correct.
  8. On 1st of month: add new checkpoint column + populate from market_price
  9. Log run stats to tcgcsv_sync_log

Environment variables required:
  SUPABASE_URL
  SUPABASE_SERVICE_KEY
"""

import asyncio
import io
import logging
import os
import time
from datetime import date, timedelta

import aiohttp
import pandas as pd
from supabase import create_client, Client

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL        = os.environ.get("SUPABASE_URL", "https://ndeklcgjbejrhoujkatn.supabase.co")
SUPABASE_KEY        = os.environ["SUPABASE_SERVICE_KEY"]
TCGCSV_BASE         = "https://tcgcsv.com/tcgplayer"
TCGCSV_LAST_UPDATED = "https://tcgcsv.com/last-updated.txt"
CONCURRENT_REQUESTS = 20
BATCH_SIZE          = 500
REQUEST_TIMEOUT     = 30
REQUEST_SLEEP       = 0.10
USER_AGENT          = "GIGA/1.0.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Supabase client ───────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ── HTTP helpers ──────────────────────────────────────────────────────────────
async def fetch_csv(session: aiohttp.ClientSession, url: str) -> pd.DataFrame | None:
    await asyncio.sleep(REQUEST_SLEEP)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
            if resp.status != 200:
                log.warning(f"HTTP {resp.status} for {url}")
                return None
            text = await resp.text()
            return pd.read_csv(io.StringIO(text), dtype=str)
    except Exception as e:
        log.warning(f"Failed to fetch {url}: {e}")
        return None


async def check_last_updated(session: aiohttp.ClientSession, supabase: Client) -> bool:
    try:
        async with session.get(
            TCGCSV_LAST_UPDATED,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            if resp.status != 200:
                log.warning("Could not fetch last-updated.txt — proceeding anyway")
                return True
            remote_ts = (await resp.text()).strip()
            log.info(f"TCGCSV last updated: {remote_ts}")
    except Exception as e:
        log.warning(f"last-updated.txt check failed: {e} — proceeding anyway")
        return True

    result = supabase.table("tcgcsv_sync_log") \
        .select("created_at") \
        .eq("status", "success") \
        .order("created_at", desc=True) \
        .limit(1) \
        .execute()

    if not result.data:
        log.info("No previous sync found — proceeding")
        return True

    last_sync = result.data[0]["created_at"]
    if remote_ts > last_sync[:len(remote_ts)]:
        log.info("TCGCSV has newer data — proceeding")
        return True
    else:
        log.info("Already up to date — skipping")
        return False


# ── Helpers ───────────────────────────────────────────────────────────────────
def safe_str(val) -> str | None:
    if pd.isna(val) or val == "" or val == "nan":
        return None
    return str(val).strip()


def safe_numeric(val) -> float | None:
    try:
        f = float(val)
        return f if not pd.isna(f) else None
    except (ValueError, TypeError):
        return None


def safe_bool(val) -> bool | None:
    if pd.isna(val) or val == "" or val == "nan":
        return None
    return str(val).strip().lower() == "true"


def upsert_batched(supabase: Client, table: str, rows: list[dict],
                   conflict_cols: str) -> int:
    if not rows:
        return 0
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        supabase.table(table).upsert(batch, on_conflict=conflict_cols).execute()
        total += len(batch)
    return total


# ── Step 1: SQL shift ─────────────────────────────────────────────────────────
def shift_all_prices(supabase: Client, today: date) -> None:
    """
    Move every market_d column forward one day for all rows in tcgcsv_prices_wide.
    market_price -> market_d1 -> market_d2 -> ... -> market_d29 (d29 is dropped).
    market_price is set to NULL after shifting — today's upsert writes the new value.
    Cards not in today's feed keep their shifted NULLs — correct.
    On 1st of month, also snapshot market_price into last month's checkpoint column.
    """
    log.info("Shifting all price columns forward one day...")

    last_month  = (today.replace(day=1) - timedelta(days=1))
    chk_col     = f"market_{last_month.year}_{last_month.month:02d}"

    shift_parts = []
    for i in range(29, 1, -1):
        shift_parts.append(f"market_d{i} = market_d{i-1}")
    shift_parts.append("market_d1 = market_price")
    shift_parts.append("market_price = NULL")
    shift_parts.append("last_updated = CURRENT_DATE")

    # On 1st of month, snapshot yesterday's price into checkpoint before clearing
    if today.day == 1:
        shift_parts.append(f"{chk_col} = market_price")
        log.info(f"  Monthly checkpoint: writing to {chk_col}")

    sql = f"UPDATE tcgcsv_prices_wide SET {', '.join(shift_parts)}"
    supabase.rpc("exec_sql", {"query": sql}).execute()
    log.info("Price shift complete.")


# ── Monthly checkpoint column ─────────────────────────────────────────────────
def ensure_checkpoint_column(supabase: Client, col_name: str) -> None:
    try:
        supabase.rpc("exec_sql", {
            "query": f"ALTER TABLE tcgcsv_prices_wide ADD COLUMN IF NOT EXISTS {col_name} numeric;"
        }).execute()
        log.info(f"Checkpoint column ensured: {col_name}")
    except Exception as e:
        log.warning(f"Could not add checkpoint column {col_name}: {e}")


# ── Groups upsert ─────────────────────────────────────────────────────────────
def upsert_groups(supabase: Client, groups_df: pd.DataFrame, category_id: int) -> int:
    rows = []
    for _, row in groups_df.iterrows():
        gid = safe_str(row.get("groupId"))
        if not gid:
            continue
        rows.append({
            "group_id":        int(gid),
            "name":            safe_str(row.get("name")),
            "abbreviation":    safe_str(row.get("abbreviation")),
            "is_supplemental": safe_bool(row.get("isSupplemental")),
            "published_on":    safe_str(row.get("publishedOn")),
            "modified_on":     safe_str(row.get("modifiedOn")),
            "category_id":     category_id,
        })
    return upsert_batched(supabase, "tcgcsv_groups", rows, "group_id")


# ── Parse products + prices ───────────────────────────────────────────────────
def parse_products_prices(df: pd.DataFrame, category_id: int, today: date) -> tuple[list, list]:
    product_rows = []
    price_rows   = []

    df.columns = [c.strip() for c in df.columns]

    required = {"productId", "subTypeName", "groupId"}
    if not required.issubset(df.columns):
        return [], []

    for _, row in df.iterrows():
        product_id   = safe_str(row.get("productId"))
        sub_type_raw = safe_str(row.get("subTypeName"))
        sub_type     = sub_type_raw or "Normal"
        group_id     = safe_str(row.get("groupId"))

        if not product_id or not group_id:
            continue

        # Skip dead listings (no subTypeName AND no prices)
        if sub_type_raw is None:
            price_vals = [row.get(c) for c in ("lowPrice", "midPrice", "highPrice", "marketPrice", "directLowPrice")]
            if all(safe_numeric(v) is None for v in price_vals):
                continue

        product_id = int(product_id)
        group_id   = int(group_id)

        product_rows.append({
            "product_id":    product_id,
            "sub_type":      sub_type,
            "category_id":   category_id,
            "group_id":      group_id,
            "name":          safe_str(row.get("name")),
            "clean_name":    safe_str(row.get("cleanName")),
            "ext_number":    safe_str(row.get("extNumber")),
            "ext_rarity":    safe_str(row.get("extRarity")),
            "ext_card_type": safe_str(row.get("extCardType")),
            "ext_hp":        safe_str(row.get("extHP")),
            "ext_stage":     safe_str(row.get("extStage")),
            "image_url":     safe_str(row.get("imageUrl")),
            "tcgcsv_url":    safe_str(row.get("url")),
            "modified_on":   safe_str(row.get("modifiedOn")),
            "updated_at":    "now()",
        })

        # Only write today's 5 price fields — shift already happened via SQL
        price_rows.append({
            "product_id":   product_id,
            "sub_type":     sub_type,
            "low_price":    safe_numeric(row.get("lowPrice")),
            "mid_price":    safe_numeric(row.get("midPrice")),
            "high_price":   safe_numeric(row.get("highPrice")),
            "market_price": safe_numeric(row.get("marketPrice")),
            "direct_low":   safe_numeric(row.get("directLowPrice")),
            "last_updated": str(today),
        })

    return product_rows, price_rows


# ── Per-group worker ──────────────────────────────────────────────────────────
async def process_group(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    category_id: int,
    group_id: str,
    today: date,
) -> tuple[list, list]:
    url = f"{TCGCSV_BASE}/{category_id}/{group_id}/ProductsAndPrices.csv"
    async with semaphore:
        df = await fetch_csv(session, url)
    if df is None or df.empty:
        return [], []
    return parse_products_prices(df, category_id, today)


# ── Per-game orchestration ────────────────────────────────────────────────────
async def process_game(
    session: aiohttp.ClientSession,
    supabase: Client,
    category_id: int,
    game_name: str,
    today: date,
) -> dict:
    log_entry = {
        "run_date":    str(today),
        "category_id": category_id,
        "game_name":   game_name,
        "status":      "running",
    }
    t0 = time.time()

    groups_url = f"{TCGCSV_BASE}/{category_id}/Groups.csv"
    groups_df  = await fetch_csv(session, groups_url)

    if groups_df is None or groups_df.empty or "groupId" not in groups_df.columns:
        log.error(f"  [{game_name}] Could not fetch groups")
        log_entry["status"]    = "error"
        log_entry["error_msg"] = "groups fetch failed"
        return log_entry

    groups_upserted = upsert_groups(supabase, groups_df, category_id)
    log.info(f"  [{game_name}] Upserted {groups_upserted} groups")

    group_ids = groups_df["groupId"].dropna().astype(str).tolist()
    log.info(f"  [{game_name}] {len(group_ids)} groups discovered")
    log_entry["groups_fetched"] = len(group_ids)

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    tasks = [
        process_group(session, semaphore, category_id, gid, today)
        for gid in group_ids
    ]
    results = await asyncio.gather(*tasks)

    all_products = []
    all_prices   = []
    for prods, prices in results:
        all_products.extend(prods)
        all_prices.extend(prices)

    log.info(f"  [{game_name}] {len(all_products):,} products, {len(all_prices):,} price rows")

    upsert_batched(supabase, "tcgcsv_products", all_products, "product_id,sub_type")
    upsert_batched(supabase, "tcgcsv_prices_wide", all_prices, "product_id,sub_type")

    log_entry["rows_upserted"]    = len(all_prices)
    log_entry["duration_seconds"] = round(time.time() - t0, 1)
    log_entry["status"]           = "success"

    supabase.table("tcgcsv_sync_config").update({
        "last_synced_at": "now()",
        "group_count":    len(group_ids),
    }).eq("category_id", category_id).execute()

    log.info(f"  [{game_name}] done in {log_entry['duration_seconds']}s")
    return log_entry


# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    today    = date.today()
    supabase = get_supabase()

    log.info(f"=== TCGCSV ingest starting — {today} ===")

    headers   = {"User-Agent": USER_AGENT}
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS + 5)

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:

        should_sync = await check_last_updated(session, supabase)
        if not should_sync:
            log.info("Nothing to do — exiting.")
            return

        # On 1st of month, ensure last month's checkpoint column exists
        if today.day == 1:
            last_month = (today.replace(day=1) - timedelta(days=1))
            chk_col    = f"market_{last_month.year}_{last_month.month:02d}"
            log.info(f"First of month — ensuring checkpoint column: {chk_col}")
            ensure_checkpoint_column(supabase, chk_col)

        # Step 1: Shift all prices forward one day
        shift_all_prices(supabase, today)

        config_resp = (
            supabase.table("tcgcsv_sync_config")
            .select("category_id, game_name")
            .eq("is_active", True)
            .order("sync_priority")
            .execute()
        )
        games = config_resp.data
        log.info(f"Active games: {[g['game_name'] for g in games]}")

        for game in games:
            log.info(f"\n── {game['game_name']} (cat {game['category_id']}) ──")
            try:
                log_entry = await process_game(
                    session, supabase,
                    game["category_id"], game["game_name"],
                    today,
                )
            except Exception as e:
                log.exception(f"  [{game['game_name']}] unhandled error: {e}")
                log_entry = {
                    "run_date":    str(today),
                    "category_id": game["category_id"],
                    "game_name":   game["game_name"],
                    "status":      "error",
                    "error_msg":   str(e),
                }

            supabase.table("tcgcsv_sync_log").insert(log_entry).execute()

    log.info("\n=== TCGCSV ingest complete ===")


if __name__ == "__main__":
    asyncio.run(main())
