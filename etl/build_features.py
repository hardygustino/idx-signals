import pandas as pd
import psycopg2, os

PG_DSN = os.getenv("PG_DSN")

def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / (loss.replace(0, 1e-9))
    return 100 - (100 / (1 + rs))

def resample_to_1h(conn):
    df = pd.read_sql("SELECT * FROM raw_prices", conn, parse_dates=["ts_utc"])
    if df.empty:
        return 0
    out = []
    for sym, g in df.groupby("symbol"):
        g = g.set_index("ts_utc").sort_index()
        o = g.resample("1H").agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"}).dropna().reset_index()
        o["symbol"] = sym
        out.append(o)
    stg = pd.concat(out)
    cur = conn.cursor()
    for r in stg.itertuples(index=False):
        cur.execute("""
            INSERT INTO stg_prices_1h(symbol, ts_utc, open, high, low, close, volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (symbol, ts_utc) DO UPDATE
            SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume;
        """, (r.symbol, r.ts_utc.to_pydatetime(), r.open, r.high, r.low, r.close, int(r.volume)))
    conn.commit(); cur.close()
    return len(stg)

def build_features(conn):
    df = pd.read_sql("SELECT symbol, ts_utc, close, volume FROM stg_prices_1h ORDER BY symbol, ts_utc", conn, parse_dates=["ts_utc"])
    if df.empty:
        return 0
    out = []
    for sym, g in df.groupby("symbol"):
        g = g.sort_values("ts_utc")
        g["ma5"] = g["close"].rolling(5).mean()
        g["ma20"] = g["close"].rolling(20).mean()
        g["rsi14"] = rsi(g["close"], 14)
        g["vol_z"] = (g["volume"] - g["volume"].rolling(20).mean()) / g["volume"].rolling(20).std(ddof=0)
        out.append(g.dropna())
    feats = pd.concat(out)
    cur = conn.cursor()
    for r in feats.itertuples(index=False):
        cur.execute("""
            INSERT INTO dwh_features_prices(symbol, ts_utc, ma5, ma20, rsi14, vol_z)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (symbol, ts_utc) DO UPDATE
            SET ma5=EXCLUDED.ma5, ma20=EXCLUDED.ma20, rsi14=EXCLUDED.rsi14, vol_z=EXCLUDED.vol_z;
        """, (r.symbol, r.ts_utc.to_pydatetime(), float(r.ma5), float(r.ma20), float(r.rsi14), float(r.vol_z)))
    conn.commit(); cur.close()
    return len(feats)

if __name__ == "__main__":
    conn = psycopg2.connect(PG_DSN)
    resample_to_1h(conn)
    build_features(conn)
    conn.close()
    print("features built successfully")
