import os
import time
import json
import math
import random
import requests
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Tuple

WB_BASE = "https://seller-analytics-api.wildberries.ru"
EP_SEARCH_TEXTS = f"{WB_BASE}/api/v2/search-report/product/search-texts"
EP_ORDERS = f"{WB_BASE}/api/v2/search-report/product/orders"

MSK_TZ = ZoneInfo("Europe/Moscow")

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return int(v)

def parse_nm_ids(s: str) -> List[int]:
    out = []
    for part in (s or "").split(","):
        part = part.strip()
        if part:
            out.append(int(part))
    return out

def msk_today() -> date:
    return datetime.now(MSK_TZ).date()

def chunked(lst: List[Any], n: int) -> List[List[Any]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def wb_headers(api_key: str) -> Dict[str, str]:
    # В документации указан HeaderApiKey; на практике WB обычно принимает Authorization: <token>
    return {
        "Authorization": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def supabase_headers(service_role_key: str) -> Dict[str, str]:
    return {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

def request_with_retry(session: requests.Session, method: str, url: str, headers: Dict[str,str], json_body: Any,
                       max_retries: int = 6) -> requests.Response:
    for attempt in range(1, max_retries + 1):
        resp = session.request(method, url, headers=headers, json=json_body, timeout=60)
        if resp.status_code != 429:
            return resp
        # 429: backoff + джиттер
        sleep_s = min(120, 20 * attempt) + random.uniform(0, 3)
        print(f"[WB] 429 Too Many Requests. Sleep {sleep_s:.1f}s (attempt {attempt}/{max_retries})")
        time.sleep(sleep_s)
    return resp

def get_top_search_texts(session: requests.Session, api_key: str, nm_id: int, start: date, end: date,
                         limit: int, top_order_by: str) -> List[str]:
    body = {
        "currentPeriod": {"start": start.isoformat(), "end": end.isoformat()},
        "nmIds": [nm_id],
        "topOrderBy": top_order_by,  # Enum: openCard/addToCart/openToCart/orders/cartToOrder :contentReference[oaicite:4]{index=4}
        "includeSubstitutedSKUs": True,
        "includeSearchTexts": True,
        "orderBy": {"field": "avgPosition", "mode": "asc"},
        "limit": limit,
    }
    resp = request_with_retry(session, "POST", EP_SEARCH_TEXTS, wb_headers(api_key), body)
    if resp.status_code != 200:
        raise RuntimeError(f"WB search-texts failed: {resp.status_code} {resp.text}")

    data = resp.json().get("data", {})
    items = data.get("items", []) or []
    texts = []
    for it in items:
        t = (it.get("text") or "").strip()
        if t:
            texts.append(t)

    # уникализируем, сохраняя порядок
    seen = set()
    uniq = []
    for t in texts:
        if t not in seen:
            uniq.append(t)
            seen.add(t)
    return uniq[:limit]

def get_orders_positions(session: requests.Session, api_key: str, nm_id: int, start: date, end: date,
                         search_texts: List[str]) -> Dict[str, Any]:
    body = {
        "period": {"start": start.isoformat(), "end": end.isoformat()},
        "nmId": nm_id,
        "searchTexts": search_texts,
    }
    resp = request_with_retry(session, "POST", EP_ORDERS, wb_headers(api_key), body)
    if resp.status_code != 200:
        raise RuntimeError(f"WB product/orders failed: {resp.status_code} {resp.text}")
    return resp.json()

def build_rows(nm_id: int, period_start: date, period_end: date, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    data = payload.get("data", {}) or {}

    # total[]: dt, avgPosition, orders :contentReference[oaicite:5]{index=5}
    for t in data.get("total", []) or []:
        rows.append({
            "load_dttm": datetime.now(tz=ZoneInfo("UTC")).isoformat(),
            "nm_id": nm_id,
            "search_text": "__TOTAL__",
            "dt": t.get("dt"),
            "avg_position": t.get("avgPosition"),
            "orders": t.get("orders"),
            "frequency": None,
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
        })

    # items[]: text, frequency, dateItems[] (dt, avgPosition, orders) :contentReference[oaicite:6]{index=6}
    for it in data.get("items", []) or []:
        text = it.get("text")
        freq = it.get("frequency")
        for di in it.get("dateItems", []) or []:
            rows.append({
                "load_dttm": datetime.now(tz=ZoneInfo("UTC")).isoformat(),
                "nm_id": nm_id,
                "search_text": text,
                "dt": di.get("dt"),
                "avg_position": di.get("avgPosition"),
                "orders": di.get("orders"),
                "frequency": freq,
                "period_start": period_start.isoformat(),
                "period_end": period_end.isoformat(),
            })

    # чистка пустых dt / text
    clean = []
    for r in rows:
        if r.get("dt") and r.get("search_text"):
            clean.append(r)
    return clean

def supabase_upsert_rows(session: requests.Session, supabase_url: str, service_role_key: str,
                        table: str, rows: List[Dict[str, Any]], batch_size: int = 500) -> None:
    if not rows:
        print("[SB] No rows to upsert")
        return

    url = f"{supabase_url.rstrip('/')}/rest/v1/{table}?on_conflict=nm_id,search_text,dt"
    headers = supabase_headers(service_role_key)

    batches = chunked(rows, batch_size)
    for i, b in enumerate(batches, start=1):
        resp = session.post(url, headers=headers, data=json.dumps(b), timeout=120)
        if resp.status_code not in (200, 201, 204):
            raise RuntimeError(f"Supabase upsert failed (batch {i}/{len(batches)}): {resp.status_code} {resp.text}")
        print(f"[SB] Upsert batch {i}/{len(batches)} ok: {len(b)} rows")

def main():
    wb_api_key = os.getenv("WB_API_KEY", "").strip()
    supabase_url = os.getenv("SUPABASE_URL", "").strip()
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    nm_ids = parse_nm_ids(os.getenv("NM_IDS", ""))

    if not wb_api_key:
        raise SystemExit("WB_API_KEY is empty")
    if not supabase_url or not supabase_key:
        raise SystemExit("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is empty")
    if not nm_ids:
        raise SystemExit("NM_IDS is empty (example: 264675414,491601909,523633405)")

    # Настройки
    lookback_days = max(1, min(7, env_int("LOOKBACK_DAYS", 3)))  # API max 7 days :contentReference[oaicite:7]{index=7}
    texts_limit = env_int("SEARCH_TEXTS_LIMIT", 30)
    texts_limit = max(1, min(100, texts_limit))
    top_order_by = os.getenv("TOP_ORDER_BY", "orders").strip() or "orders"
    pause_seconds = float(os.getenv("WB_REQUEST_PAUSE_SEC", "21"))

    # Период: по MSK, до "вчера" включительно
    end = msk_today() - timedelta(days=1)
    start = end - timedelta(days=lookback_days - 1)

    print(f"[RUN] nm_ids={nm_ids}")
    print(f"[RUN] period={start.isoformat()}..{end.isoformat()} (MSK-based)")
    print(f"[RUN] texts_limit={texts_limit}, top_order_by={top_order_by}, lookback_days={lookback_days}")

    all_rows: List[Dict[str, Any]] = []
    with requests.Session() as s:
        for nm_id in nm_ids:
            # 1) Берём топ запросов по товару :contentReference[oaicite:8]{index=8}
            print(f"\n[WB] Fetch top search texts for nmId={nm_id}")
            texts = get_top_search_texts(s, wb_api_key, nm_id, start, end, texts_limit, top_order_by)
            print(f"[WB] Got {len(texts)} search texts")

            if not texts:
                continue

            # 2) Тянем orders+позиции по этим запросам :contentReference[oaicite:9]{index=9}
            print(f"[WB] Fetch orders/positions for nmId={nm_id}")
            payload = get_orders_positions(s, wb_api_key, nm_id, start, end, texts)
            rows = build_rows(nm_id, start, end, payload)
            print(f"[WB] Parsed {len(rows)} rows")
            all_rows.extend(rows)

            # лимит 3/мин → пауза
            time.sleep(pause_seconds)

    print(f"\n[SB] Total rows to upsert: {len(all_rows)}")
    with requests.Session() as s2:
        supabase_upsert_rows(
            s2, supabase_url, supabase_key,
            table="wb_search_orders_positions",
            rows=all_rows,
            batch_size=500
        )

    print("[DONE]")

if __name__ == "__main__":
    main()
