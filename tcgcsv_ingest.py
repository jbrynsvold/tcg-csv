"""
tcgcsv_ingest.py
----------------
Daily ingest of TCGplayer prices via TCGCSV.com into Supabase.

Flow:
  1. Check last-updated.txt — skip if already synced today
  2. Load active games from tcgcsv_sync_config
  3. For each game, fetch Groups.csv to discover all set IDs dynamically
  4. Download each group's ProductsAndPrices.csv (async, 20 concurrent)
  5. Upsert products → tcgcsv_products
  6. Shift price columns → tcgcsv_prices_wide (d1→d2, d2→d3 ... d28→d29)
  7. Write today's 5 price fields → tcgcsv_prices_wide
  8. On 1st of month: add new checkpoint column + populate from market_d1
  9. Log run stats to tcgcsv_sync_log

Environment variables required:
  SUPABASE_URL
  SUPABASE_SERVICE_KEY

Run:  python tcgcsv_ingest.py
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

# ── Config ───────────────────────────────────────────────────────────────────
SUPABASE_URL        = os.environ["SUPABASE_URL"]
SUPABASE_KEY        = os.environ["SUPABASE_SERVICE_KEY"]
TCGCSV_BASE         = "https://tcgcsv.com/tcgplayer"
TCGCSV_LAST_UPDATED = "https://tcgcsv.com/last-updated.txt"
CONCURRENT_REQUESTS = 20
BATCH_SIZE          = 500
REQUEST_TIMEOUT     = 30
REQUEST_SLEEP       = 0.10         # 100ms between requests per usage guidelines
USER_AGENT          = "GIGA/1.0.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Rolling day columns d1-d29 (d1 = yesterday, d29 = 29 days ago)
DAY_COLS = [f"market_d{i}" for i in range(1, 30)]


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
        batch = rows[i : i + BATCH_SIZE]
        supabase.table(table).upsert(batch, on_conflict=conflict_cols).execute()
        total += len(batch)
    return total


# ── Monthly checkpoint management ────────────────────────────────────────────
def get_checkpoint_col(d: date) -> str:
    """Return checkpoint column name for a given month, e.g. 'market_2026_04'"""
    return f"market_{d.year}_{d.month:02d}"


def ensure_checkpoint_column(supabase: Client, col_name: str) -> None:
    """Add checkpoint column if it doesn't exist yet."""
    try:
        # Try a direct SQL alter via RPC — if column exists this will error
        supabase.rpc("exec_sql", {
            "query": f"ALTER TABLE tcgcsv_prices_wide ADD COLUMN IF NOT EXISTS {col_name} numeric;"
        }).execute()
        log.info(f"  Checkpoint column ensured: {col_name}")
    except Exception as e:
        log.warning(f"  Could not add checkpoint column {col_name}: {e}")


# ── Price shift + write ───────────────────────────────────────────────────────
def build_shift_sql(today: date) -> str:
    """
    Build SQL that:
    1. Shifts d1→d2, d2→d3 ... d28→d29 (drops d29, it's now 30 days old)
    2. Writes market_price → market_d1 (yesterday becomes d1)
    On 1st of month: also writes market_d1 → checkpoint column for last month
    """
    yesterday = today - timedelta(days=1)
    last_month = (today.replace(day=1) - timedelta(days=1))  # last day of prev month
    checkpoint_col = get_checkpoint_col(last_month)

    # Build shift: d28→d29, d27→d28, ..., d1→d2
    shifts = []
    for i in range(29, 1, -1):
        shifts.append(f"market_d{i} = market_d{i-1}")

    # Yesterday's market price → d1
    shifts.append("market_d1 = market_price")

    # On 1st of month, snapshot yesterday's price into checkpoint
    if today.day == 1:
        shifts.append(f"{checkpoint_col} = market_price")
        log.info(f"  Monthly checkpoint: writing to {checkpoint_col}")

    shifts.append("last_updated = CURRENT_DATE")

    return ", ".join(shifts)


def write_prices_sql(today: date, price_rows: list[dict]) -> None:
    """
    We use raw SQL via Supabase RPC for the wide table upsert because
    we need to shift columns atomically and write today's 5 fields.
    Returns SQL string for batch execution.
    """
    pass  # handled via upsert_batched below


def parse_products_prices(df: pd.DataFrame, category_id: int, today: date) -> tuple[list, list]:
    """
    Parse a ProductsAndPrices DataFrame.
    Returns:
      - product_rows  → tcgcsv_products upsert
      - price_rows    → tcgcsv_prices_wide upsert
    """
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
        log_entry["status"] = "error"
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

    # Upsert products (metadata)
    upsert_batched(supabase, "tcgcsv_products", all_products, "product_id,sub_type")

    # Upsert prices into wide table
    # On conflict: shift day columns, update today's 5 fields
    # We use a custom upsert that sets the shift via the merge_duplicates logic
    upsert_wide_prices(supabase, all_prices, today)

    log_entry["rows_upserted"]    = len(all_prices)
    log_entry["duration_seconds"] = round(time.time() - t0, 1)
    log_entry["status"]           = "success"

    supabase.table("tcgcsv_sync_config").update({
        "last_synced_at": "now()",
        "group_count":    len(group_ids),
    }).eq("category_id", category_id).execute()

    log.info(f"  [{game_name}] done in {log_entry['duration_seconds']}s")
    return log_entry


# ── Wide table upsert with column shift ──────────────────────────────────────
def upsert_wide_prices(supabase: Client, price_rows: list[dict], today: date) -> None:
    """
    Upsert into tcgcsv_prices_wide.
    - New rows: insert with today's 5 fields, all day columns null
    - Existing rows: shift d1→d2...d28→d29, today's market→d1, write new 5 fields
    We do this via raw SQL in batches for performance.
    """
    if not price_rows:
        return

    yesterday   = today - timedelta(days=1)
    last_month  = (today.replace(day=1) - timedelta(days=1))
    chk_col     = get_checkpoint_col(last_month)
    is_month_1  = today.day == 1

    # Build the ON CONFLICT UPDATE shift expression
    shift_parts = []
    for i in range(29, 1, -1):
        shift_parts.append(f"market_d{i} = EXCLUDED.market_d{i-1} -- will be set from existing")

    # We do this properly: fetch existing market_price for these product_ids,
    # then build upsert rows that include the shifted values.
    # For simplicity and performance, use a SQL UPDATE after insert.

    for i in range(0, len(price_rows), BATCH_SIZE):
        batch = price_rows[i : i + BATCH_SIZE]

        # Step 1: Insert new rows (new products get today's prices, no history)
        insert_rows = [{
            "product_id":   r["product_id"],
            "sub_type":     r["sub_type"],
            "low_price":    r["low_price"],
            "mid_price":    r["mid_price"],
            "high_price":   r["high_price"],
            "market_price": r["market_price"],
            "direct_low":   r["direct_low"],
            "last_updated": r["last_updated"],
        } for r in batch]

        # Step 2: For existing rows, use upsert but we need the shift
        # Build upsert with explicit conflict handling via SQL
        product_ids = [r["product_id"] for r in batch]
        sub_types   = list({r["sub_type"] for r in batch})

        # Fetch current market prices to use as d1
        existing = supabase.table("tcgcsv_prices_wide") \
            .select("product_id,sub_type,market_price,market_d1,market_d2,market_d3,market_d4,market_d5,market_d6,market_d7,market_d8,market_d9,market_d10,market_d11,market_d12,market_d13,market_d14,market_d15,market_d16,market_d17,market_d18,market_d19,market_d20,market_d21,market_d22,market_d23,market_d24,market_d25,market_d26,market_d27,market_d28") \
            .in_("product_id", product_ids) \
            .execute()

        existing_map = {
            (r["product_id"], r["sub_type"]): r
            for r in (existing.data or [])
        }

        upsert_rows = []
        for r in batch:
            key = (r["product_id"], r["sub_type"])
            ex  = existing_map.get(key)

            row = {
                "product_id":   r["product_id"],
                "sub_type":     r["sub_type"],
                "low_price":    r["low_price"],
                "mid_price":    r["mid_price"],
                "high_price":   r["high_price"],
                "market_price": r["market_price"],
                "direct_low":   r["direct_low"],
                "last_updated": r["last_updated"],
            }

            if ex:
                # Shift existing day columns
                row["market_d1"]  = ex.get("market_price")   # yesterday → d1
                row["market_d2"]  = ex.get("market_d1")
                row["market_d3"]  = ex.get("market_d2")
                row["market_d4"]  = ex.get("market_d3")
                row["market_d5"]  = ex.get("market_d4")
                row["market_d6"]  = ex.get("market_d5")
                row["market_d7"]  = ex.get("market_d6")
                row["market_d8"]  = ex.get("market_d7")
                row["market_d9"]  = ex.get("market_d8")
                row["market_d10"] = ex.get("market_d9")
                row["market_d11"] = ex.get("market_d10")
                row["market_d12"] = ex.get("market_d11")
                row["market_d13"] = ex.get("market_d12")
                row["market_d14"] = ex.get("market_d13")
                row["market_d15"] = ex.get("market_d14")
                row["market_d16"] = ex.get("market_d15")
                row["market_d17"] = ex.get("market_d16")
                row["market_d18"] = ex.get("market_d17")
                row["market_d19"] = ex.get("market_d18")
                row["market_d20"] = ex.get("market_d19")
                row["market_d21"] = ex.get("market_d20")
                row["market_d22"] = ex.get("market_d21")
                row["market_d23"] = ex.get("market_d22")
                row["market_d24"] = ex.get("market_d23")
                row["market_d25"] = ex.get("market_d24")
                row["market_d26"] = ex.get("market_d25")
                row["market_d27"] = ex.get("market_d26")
                row["market_d28"] = ex.get("market_d27")
                row["market_d29"] = ex.get("market_d28")
                # d29 is dropped (>30 days old)

                # Monthly checkpoint on 1st of month
                if is_month_1:
                    row[chk_col] = ex.get("market_price")

            upsert_rows.append(row)

        supabase.table("tcgcsv_prices_wide").upsert(
            upsert_rows, on_conflict="product_id,sub_type"
        ).execute()


# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    today    = date.today()
    supabase = get_supabase()

    log.info(f"=== TCGCSV ingest starting — {today} ===")

    # On 1st of month, ensure this month's checkpoint column exists
    if today.day == 1:
        last_month  = (today.replace(day=1) - timedelta(days=1))
        chk_col     = get_checkpoint_col(last_month)
        log.info(f"First of month — ensuring checkpoint column: {chk_col}")
        try:
            supabase.rpc("exec_sql", {
                "query": f"ALTER TABLE tcgcsv_prices_wide ADD COLUMN IF NOT EXISTS {chk_col} numeric;"
            }).execute()
        except Exception as e:
            log.warning(f"Could not add checkpoint column via RPC: {e}")
            log.warning("Please add manually: ALTER TABLE tcgcsv_prices_wide ADD COLUMN IF NOT EXISTS {chk_col} numeric;")

    headers   = {"User-Agent": USER_AGENT}
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS + 5)

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:

        should_sync = await check_last_updated(session, supabase)
        if not should_sync:
            log.info("Nothing to do — exiting.")
            return

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
