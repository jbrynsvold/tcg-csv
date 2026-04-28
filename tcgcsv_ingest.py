"""
tcgcsv_ingest.py
----------------
Daily ingest of TCGplayer prices via TCGCSV.com into Supabase.

Flow:
  1. Load active games from tcgcsv_sync_config
  2. For each game, fetch Groups.csv to discover all set IDs dynamically
  3. Download each group's ProductsAndPrices.csv (async, 20 concurrent)
  4. Upsert products → tcgcsv_products (skip if modified_on unchanged)
  5. Upsert prices  → tcgcsv_prices_latest (always overwrite)
  6. Insert prices  → tcgcsv_prices_history (on conflict do nothing)
  7. Purge history older than 30 days (preserve checkpoints)
  8. On 1st of month: mark yesterday as checkpoint before purge
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
CONCURRENT_REQUESTS = 20
BATCH_SIZE          = 500          # rows per Supabase upsert call
HISTORY_DAYS        = 30           # rolling window before purge
REQUEST_TIMEOUT     = 30           # seconds per HTTP request

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
    """Fetch a CSV URL and return a DataFrame, or None on failure."""
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


# ── Supabase upsert helpers ───────────────────────────────────────────────────
def upsert_batched(supabase: Client, table: str, rows: list[dict],
                   conflict_cols: str, update_cols: list[str] | None = None) -> int:
    """Upsert rows in batches of BATCH_SIZE. Returns total rows processed."""
    if not rows:
        return 0
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        supabase.table(table).upsert(batch, on_conflict=conflict_cols).execute()
        total += len(batch)
    return total


def insert_ignore_batched(supabase: Client, table: str, rows: list[dict]) -> int:
    """Insert rows, ignoring conflicts. Returns total rows processed."""
    if not rows:
        return 0
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        # ignoreDuplicates=True maps to ON CONFLICT DO NOTHING
        supabase.table(table).upsert(
            batch,
            on_conflict="product_id,sub_type,price_date",
            ignore_duplicates=True,
        ).execute()
        total += len(batch)
    return total


# ── Data transformation ───────────────────────────────────────────────────────
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


def parse_products_prices(df: pd.DataFrame, category_id: int, today: date) -> tuple[list, list, list]:
    """
    Parse a ProductsAndPrices DataFrame into three lists:
      - product_rows   → tcgcsv_products upsert
      - latest_rows    → tcgcsv_prices_latest upsert
      - history_rows   → tcgcsv_prices_history insert-ignore
    """
    product_rows = []
    latest_rows  = []
    history_rows = []

    # Normalize column names (files differ slightly in order)
    df.columns = [c.strip() for c in df.columns]

    required = {"productId", "subTypeName", "groupId"}
    if not required.issubset(df.columns):
        log.warning(f"  Missing required columns: {required - set(df.columns)}")
        return [], [], []

    for _, row in df.iterrows():
        product_id   = safe_str(row.get("productId"))
        sub_type_raw = safe_str(row.get("subTypeName"))
        sub_type     = sub_type_raw or "Normal"
        group_id     = safe_str(row.get("groupId"))

        if not product_id or not group_id:
            continue

        # Skip rows with no subTypeName AND no prices — these are dead listings
        # (code cards, unlisted blisters) that will never have market data.
        # Rows with a real subTypeName are always kept even if prices are temporarily null.
        if sub_type_raw is None:
            price_vals = [row.get(c) for c in ("lowPrice", "midPrice", "highPrice", "marketPrice", "directLowPrice")]
            if all(safe_numeric(v) is None for v in price_vals):
                continue

        product_id = int(product_id)
        group_id   = int(group_id)

        # ── product record ──
        product_rows.append({
            "product_id":   product_id,
            "sub_type":     sub_type,
            "category_id":  category_id,
            "group_id":     group_id,
            "name":         safe_str(row.get("name")),
            "clean_name":   safe_str(row.get("cleanName")),
            "ext_number":   safe_str(row.get("extNumber")),
            "ext_rarity":   safe_str(row.get("extRarity")),
            "ext_card_type":safe_str(row.get("extCardType")),
            "ext_hp":       safe_str(row.get("extHP")),
            "ext_stage":    safe_str(row.get("extStage")),
            "image_url":    safe_str(row.get("imageUrl")),
            "tcgcsv_url":   safe_str(row.get("url")),
            "modified_on":  safe_str(row.get("modifiedOn")),
            "updated_at":   "now()",
        })

        # ── price records ──
        price = {
            "product_id":  product_id,
            "sub_type":    sub_type,
            "low_price":   safe_numeric(row.get("lowPrice")),
            "mid_price":   safe_numeric(row.get("midPrice")),
            "high_price":  safe_numeric(row.get("highPrice")),
            "market_price":safe_numeric(row.get("marketPrice")),
            "direct_low":  safe_numeric(row.get("directLowPrice")),
            "price_date":  str(today),
        }

        latest_rows.append({**price, "updated_at": "now()"})
        history_rows.append(price)

    return product_rows, latest_rows, history_rows


# ── Core per-group worker ─────────────────────────────────────────────────────
async def process_group(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    category_id: int,
    group_id: str,
    today: date,
) -> tuple[list, list, list]:
    """Download one group's CSV and parse it. Returns (products, latest, history)."""
    url = f"{TCGCSV_BASE}/{category_id}/{group_id}/ProductsAndPrices.csv"
    async with semaphore:
        df = await fetch_csv(session, url)

    if df is None or df.empty:
        return [], [], []

    return parse_products_prices(df, category_id, today)


# ── Per-game orchestration ────────────────────────────────────────────────────
async def process_game(
    session: aiohttp.ClientSession,
    supabase: Client,
    category_id: int,
    game_name: str,
    today: date,
) -> dict:
    """Fetch all groups for a game, download all CSVs, upsert to Supabase."""
    log_entry = {
        "run_date":    str(today),
        "category_id": category_id,
        "game_name":   game_name,
        "status":      "running",
    }
    t0 = time.time()

    # ── Discover groups ──
    groups_url = f"{TCGCSV_BASE}/{category_id}/Groups.csv"
    groups_df  = await fetch_csv(session, groups_url)

    if groups_df is None or groups_df.empty or "groupId" not in groups_df.columns:
        log.error(f"  [{game_name}] Could not fetch groups from {groups_url}")
        log_entry["status"] = "error"
        log_entry["error_msg"] = "groups fetch failed"
        return log_entry

    group_ids = groups_df["groupId"].dropna().astype(str).tolist()
    log.info(f"  [{game_name}] {len(group_ids)} groups discovered")
    log_entry["groups_fetched"] = len(group_ids)

    # ── Download all groups concurrently ──
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    tasks = [
        process_group(session, semaphore, category_id, gid, today)
        for gid in group_ids
    ]
    results = await asyncio.gather(*tasks)

    all_products = []
    all_latest   = []
    all_history  = []
    for prods, latest, history in results:
        all_products.extend(prods)
        all_latest.extend(latest)
        all_history.extend(history)

    log.info(f"  [{game_name}] {len(all_products):,} product rows, {len(all_latest):,} price rows")

    # ── Upsert to Supabase ──
    upsert_batched(supabase, "tcgcsv_products",      all_products, "product_id,sub_type")
    upsert_batched(supabase, "tcgcsv_prices_latest", all_latest,   "product_id,sub_type")
    inserted = insert_ignore_batched(supabase, "tcgcsv_prices_history", all_history)

    log_entry["rows_upserted"] = len(all_latest)
    log_entry["duration_seconds"] = round(time.time() - t0, 1)
    log_entry["status"] = "success"

    # ── Update last_synced_at in config ──
    supabase.table("tcgcsv_sync_config").update({
        "last_synced_at": "now()",
        "group_count": len(group_ids),
    }).eq("category_id", category_id).execute()

    log.info(f"  [{game_name}] done in {log_entry['duration_seconds']}s")
    return log_entry


# ── History maintenance ───────────────────────────────────────────────────────
def maintain_history(supabase: Client, today: date) -> int:
    """
    1. On 1st of month: mark yesterday's rows as monthly checkpoints.
    2. Delete rows older than HISTORY_DAYS that are not checkpoints.
    Returns count of purged rows (approximate — Supabase doesn't return delete count easily).
    """
    yesterday  = today - timedelta(days=1)
    cutoff     = today - timedelta(days=HISTORY_DAYS)

    # Monthly checkpoint
    if today.day == 1:
        log.info(f"  Marking {yesterday} as monthly checkpoint...")
        supabase.table("tcgcsv_prices_history").update({"is_checkpoint": True}).eq(
            "price_date", str(yesterday)
        ).eq("is_checkpoint", False).execute()

    # Purge old non-checkpoint rows
    log.info(f"  Purging history older than {cutoff} (non-checkpoints)...")
    supabase.table("tcgcsv_prices_history").delete().lt(
        "price_date", str(cutoff)
    ).eq("is_checkpoint", False).execute()

    return 0  # Supabase REST doesn't return delete row count


# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    today    = date.today()
    supabase = get_supabase()

    log.info(f"=== TCGCSV ingest starting — {today} ===")

    # Load active games ordered by priority
    config_resp = (
        supabase.table("tcgcsv_sync_config")
        .select("category_id, game_name")
        .eq("is_active", True)
        .order("sync_priority")
        .execute()
    )
    games = config_resp.data
    log.info(f"Active games: {[g['game_name'] for g in games]}")

    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS + 5)
    async with aiohttp.ClientSession(connector=connector) as session:
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

    # History maintenance after all games done
    log.info("\n── History maintenance ──")
    maintain_history(supabase, today)

    log.info("\n=== TCGCSV ingest complete ===")


if __name__ == "__main__":
    asyncio.run(main())
