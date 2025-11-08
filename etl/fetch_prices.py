import os
import pandas as pd
import psycopg2
import yfinance as yf

TICKERS = os.getenv("TICKERS", "WIIM.JK,TOBA.JK,ADRO.JK,BBCA.JK,HMSP.JK,BBRI.JK").split(",")
PG_DSN = os.getenv("PG_DSN")
DEBUG = os.getenv("DEBUG_FETCH", "0") == "1"

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Flatten MultiIndex → "open_wiim.jk" dst, dan lowercase
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            "_".join([str(x) for x in tup if x not in (None, "")]).lower().replace(" ", "_")
            for tup in df.columns.to_list()
        ]
    else:
        df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
    return df

def _pick(colnames, want: str) -> str:
    """
    Cari kolom untuk 'open'/'high'/'low'/'close'/'volume'.
    Support: open, open_*, *_open, adj_close, adj_close_*, *_adj_close, vol, *_vol, *_volume.
    """
    want = want.lower()
    cols = list(colnames)

    # 1) exact
    if want in colnames:
        return want

    # 2) startswith (open_*, high_* ...)
    for c in cols:
        if c.startswith(want + "_"):
            return c

    # 3) endswith (*_open)
    for c in cols:
        if c.endswith("_" + want):
            return c

    # 4) special cases
    if want == "close":
        if "adj_close" in colnames:
            return "adj_close"
        for c in cols:
            if c.startswith("adj_close_") or c.endswith("_adj_close"):
                return c

    if want == "volume":
        if "vol" in colnames:
            return "vol"
        for c in cols:
            if c.startswith("volume_") or c.endswith("_volume") or c.startswith("vol_") or c.endswith("_vol"):
                return c

    raise KeyError(f"cannot find column for '{want}' in: {cols}")

def fetch_last_hours(interval="60m", lookback_days=3) -> pd.DataFrame:
    frames = []
    for t in [s.strip() for s in TICKERS if s.strip()]:
        df = yf.download(
            t,
            period=f"{lookback_days}d",
            interval=interval,
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        if df is None or df.empty:
            if DEBUG:
                print(f"[DEBUG] empty df for {t}")
            continue

        df = df.reset_index()
        _normalize_columns(df)

        # timestamp column
        ts_col = next((c for c in ("datetime", "date", "index") if c in df.columns), None)
        if ts_col is None:
            df["index"] = df.index
            ts_col = "index"

        # robust OHLCV detection
        open_col  = _pick(df.columns, "open")
        high_col  = _pick(df.columns, "high")
        low_col   = _pick(df.columns, "low")
        close_col = _pick(df.columns, "close")
        vol_col   = _pick(df.columns, "volume")

        out = pd.DataFrame({
            "symbol": t,
            "ts_utc": pd.to_datetime(df[ts_col], utc=True),
            "open":   pd.to_numeric(df[open_col],  errors="coerce"),
            "high":   pd.to_numeric(df[high_col],  errors="coerce"),
            "low":    pd.to_numeric(df[low_col],   errors="coerce"),
            "close":  pd.to_numeric(df[close_col], errors="coerce"),
            "volume": pd.to_numeric(df[vol_col],   errors="coerce").fillna(0).astype("Int64"),
        }).dropna(subset=["ts_utc", "open", "high", "low", "close"])

        frames.append(out)

        if DEBUG:
            print(f"[DEBUG] {t} cols:", list(df.columns)[:20])

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def upsert_raw_prices(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()
    cur.execute("SET TIME ZONE 'UTC';")
    rows = 0
    for r in df.itertuples(index=False):
        cur.execute(
            """
            INSERT INTO raw_prices(symbol, ts_utc, open, high, low, close, volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (symbol, ts_utc) DO UPDATE
            SET open=EXCLUDED.open,
                high=EXCLUDED.high,
                low=EXCLUDED.low,
                close=EXCLUDED.close,
                volume=EXCLUDED.volume;
            """,
            (
                r.symbol,
                pd.to_datetime(r.ts_utc).to_pydatetime(),
                None if pd.isna(r.open)  else float(r.open),
                None if pd.isna(r.high)  else float(r.high),
                None if pd.isna(r.low)   else float(r.low),
                None if pd.isna(r.close) else float(r.close),
                int(r.volume) if pd.notna(r.volume) else 0,
            ),
        )
        rows += 1
    conn.commit()
    cur.close()
    conn.close()
    return rows

if __name__ == "__main__":
    df = fetch_last_hours()
    print("fetched rows:", 0 if df is None else len(df))
    print("upserted:", upsert_raw_prices(df))
