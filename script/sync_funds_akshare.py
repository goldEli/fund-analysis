import argparse
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if not key:
            continue
        os.environ.setdefault(key, value)


def connect_pg():
    try:
        import psycopg2  # type: ignore
    except ModuleNotFoundError:
        print("Missing dependency: psycopg2-binary", file=sys.stderr)
        print("Install: python -m pip install -r script/requirements.txt", file=sys.stderr)
        raise

    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url:
        return psycopg2.connect(database_url)

    host = os.getenv("PGHOST", "127.0.0.1")
    port = os.getenv("PGPORT", "5432")
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "")
    dbname = os.getenv("PGDATABASE", "postgres")
    return psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)


def ensure_schema(conn, schema: str) -> None:
    from psycopg2 import sql  # type: ignore

    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))


def ensure_tables(conn, schema: str) -> None:
    from psycopg2 import sql  # type: ignore

    ensure_schema(conn, schema)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.fund_basic (
                    fund_code TEXT PRIMARY KEY,
                    fund_name TEXT,
                    fund_type TEXT,
                    return_1m DOUBLE PRECISION,
                    return_3m DOUBLE PRECISION,
                    return_1y DOUBLE PRECISION,
                    peer_avg_return_1m DOUBLE PRECISION,
                    peer_avg_return_3m DOUBLE PRECISION,
                    peer_avg_return_1y DOUBLE PRECISION,
                    peer_rank_1m INTEGER,
                    peer_rank_3m INTEGER,
                    peer_rank_1y INTEGER,
                    peer_count_1m INTEGER,
                    peer_count_3m INTEGER,
                    peer_count_1y INTEGER,
                    peer_rank_1m_display TEXT,
                    peer_rank_3m_display TEXT,
                    peer_rank_1y_display TEXT,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """
            ).format(sql.Identifier(schema))
        )
        for ddl in (
            "ALTER TABLE {}.fund_basic ADD COLUMN IF NOT EXISTS return_1m DOUBLE PRECISION",
            "ALTER TABLE {}.fund_basic ADD COLUMN IF NOT EXISTS return_3m DOUBLE PRECISION",
            "ALTER TABLE {}.fund_basic ADD COLUMN IF NOT EXISTS return_1y DOUBLE PRECISION",
            "ALTER TABLE {}.fund_basic ADD COLUMN IF NOT EXISTS peer_avg_return_1m DOUBLE PRECISION",
            "ALTER TABLE {}.fund_basic ADD COLUMN IF NOT EXISTS peer_avg_return_3m DOUBLE PRECISION",
            "ALTER TABLE {}.fund_basic ADD COLUMN IF NOT EXISTS peer_avg_return_1y DOUBLE PRECISION",
            "ALTER TABLE {}.fund_basic ADD COLUMN IF NOT EXISTS peer_rank_1m INTEGER",
            "ALTER TABLE {}.fund_basic ADD COLUMN IF NOT EXISTS peer_rank_3m INTEGER",
            "ALTER TABLE {}.fund_basic ADD COLUMN IF NOT EXISTS peer_rank_1y INTEGER",
            "ALTER TABLE {}.fund_basic ADD COLUMN IF NOT EXISTS peer_count_1m INTEGER",
            "ALTER TABLE {}.fund_basic ADD COLUMN IF NOT EXISTS peer_count_3m INTEGER",
            "ALTER TABLE {}.fund_basic ADD COLUMN IF NOT EXISTS peer_count_1y INTEGER",
            "ALTER TABLE {}.fund_basic ADD COLUMN IF NOT EXISTS peer_rank_1m_display TEXT",
            "ALTER TABLE {}.fund_basic ADD COLUMN IF NOT EXISTS peer_rank_3m_display TEXT",
            "ALTER TABLE {}.fund_basic ADD COLUMN IF NOT EXISTS peer_rank_1y_display TEXT",
        ):
            cur.execute(sql.SQL(ddl).format(sql.Identifier(schema)))
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.fund_nav (
                    fund_code TEXT NOT NULL,
                    nav_date DATE NOT NULL,
                    unit_nav DOUBLE PRECISION,
                    daily_growth_rate DOUBLE PRECISION,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (fund_code, nav_date)
                )
                """
            ).format(sql.Identifier(schema))
        )
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS idx_fund_nav_code ON {}.fund_nav (fund_code)").format(
                sql.Identifier(schema)
            )
        )


def parse_date(value: str) -> date:
    v = value.strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Invalid date: {value}")


def parse_codes(value: str) -> List[str]:
    parts = [p.strip() for p in value.split(",") if p.strip()]
    return parts


def normalize_fund_code(value) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) < 6:
        s = s.zfill(6)
    return s


def fetch_fund_basic():
    try:
        import akshare as ak  # type: ignore
    except ModuleNotFoundError:
        print("Missing dependency: akshare", file=sys.stderr)
        print("Install: python -m pip install -r script/requirements.txt", file=sys.stderr)
        raise

    return ak.fund_name_em()


def fetch_fund_rank():
    try:
        import akshare as ak  # type: ignore
    except ModuleNotFoundError:
        print("Missing dependency: akshare", file=sys.stderr)
        print("Install: python -m pip install -r script/requirements.txt", file=sys.stderr)
        raise

    return ak.fund_open_fund_rank_em(symbol="全部")


def fetch_fund_money_rank():
    try:
        import akshare as ak  # type: ignore
    except ModuleNotFoundError:
        print("Missing dependency: akshare", file=sys.stderr)
        print("Install: python -m pip install -r script/requirements.txt", file=sys.stderr)
        raise

    return ak.fund_money_rank_em()


def fetch_fund_exchange_rank():
    try:
        import akshare as ak  # type: ignore
    except ModuleNotFoundError:
        print("Missing dependency: akshare", file=sys.stderr)
        print("Install: python -m pip install -r script/requirements.txt", file=sys.stderr)
        raise

    return ak.fund_exchange_rank_em()


def to_int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        if str(value).strip() == "":
            return None
        return int(float(value))
    except Exception:
        return None


def format_rank_display(rank: Optional[int], count: Optional[int]) -> Optional[str]:
    if rank is None or count is None:
        return None
    return f"{rank}/{count}"


def normalize_fund_name(value) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""
    return s


def make_name_aliases(name: str) -> List[str]:
    aliases = {name}
    if "(后端)" in name:
        aliases.add(name.replace("(后端)", "").strip())
    if "（后端）" in name:
        aliases.add(name.replace("（后端）", "").strip())
    return [item for item in aliases if item]


def prepare_rank_df(rank_df, source_name: str):
    try:
        import pandas as pd  # type: ignore
    except ModuleNotFoundError:
        print("Missing dependency: pandas", file=sys.stderr)
        print("Install: python -m pip install -r script/requirements.txt", file=sys.stderr)
        raise

    if rank_df is None or getattr(rank_df, "empty", False):
        return pd.DataFrame()

    ranked = rank_df.copy()
    if "基金代码" in ranked.columns:
        ranked["基金代码"] = ranked["基金代码"].map(normalize_fund_code)
    if "基金简称" in ranked.columns:
        ranked["基金简称"] = ranked["基金简称"].map(normalize_fund_name)
    for col in ("近1月", "近3月", "近1年"):
        if col not in ranked.columns:
            ranked[col] = None
        ranked[col] = pd.to_numeric(ranked[col], errors="coerce")
    ranked["rank_source"] = source_name
    return ranked


def apply_rank_metrics(records: Dict[str, Dict[str, object]], ranked, source_name: str) -> int:
    try:
        import pandas as pd  # type: ignore
    except ModuleNotFoundError:
        print("Missing dependency: pandas", file=sys.stderr)
        print("Install: python -m pip install -r script/requirements.txt", file=sys.stderr)
        raise

    if ranked is None or getattr(ranked, "empty", False):
        return 0

    matched_codes = 0
    ranked = ranked.copy()
    ranked["基金类型"] = ranked["基金代码"].map(
        lambda code: records.get(code, {}).get("fund_type")
    )

    for period_col, suffix in (("近1月", "1m"), ("近3月", "3m"), ("近1年", "1y")):
        valid = ranked[ranked["基金类型"].notna() & ranked[period_col].notna()].copy()
        if valid.empty:
            continue
        valid[f"peer_avg_return_{suffix}"] = valid.groupby("基金类型")[period_col].transform("mean")
        valid[f"peer_count_{suffix}"] = valid.groupby("基金类型")[period_col].transform("count")
        valid[f"peer_rank_{suffix}"] = valid.groupby("基金类型")[period_col].rank(
            method="min", ascending=False
        )
        for _, r in valid.iterrows():
            code = r["基金代码"]
            if not code or code not in records:
                continue
            rec = records[code]
            if rec.get("rank_source") is None:
                matched_codes += 1
                rec["rank_source"] = source_name
            rec[f"return_{suffix}"] = to_float(r.get(period_col))
            rec[f"peer_avg_return_{suffix}"] = to_float(r.get(f"peer_avg_return_{suffix}"))
            rec[f"peer_rank_{suffix}"] = to_int(r.get(f"peer_rank_{suffix}"))
            rec[f"peer_count_{suffix}"] = to_int(r.get(f"peer_count_{suffix}"))
            rec[f"peer_rank_{suffix}_display"] = format_rank_display(
                rec.get(f"peer_rank_{suffix}"),
                rec.get(f"peer_count_{suffix}"),
            )
    return matched_codes


def fill_rank_metrics_by_name_alias(records: Dict[str, Dict[str, object]]) -> int:
    metrics = (
        "return_1m",
        "return_3m",
        "return_1y",
        "peer_avg_return_1m",
        "peer_avg_return_3m",
        "peer_avg_return_1y",
        "peer_rank_1m",
        "peer_rank_3m",
        "peer_rank_1y",
        "peer_count_1m",
        "peer_count_3m",
        "peer_count_1y",
        "peer_rank_1m_display",
        "peer_rank_3m_display",
        "peer_rank_1y_display",
    )

    alias_map: Dict[Tuple[str, Optional[str]], Dict[str, object]] = {}
    for rec in records.values():
        name = normalize_fund_name(rec.get("fund_name"))
        ftype = rec.get("fund_type")
        if not name or rec.get("return_1m") is None:
            continue
        for alias in make_name_aliases(name):
            alias_map.setdefault((alias, ftype), rec)

    filled = 0
    for rec in records.values():
        if rec.get("return_1m") is not None:
            continue
        name = normalize_fund_name(rec.get("fund_name"))
        ftype = rec.get("fund_type")
        if not name:
            continue
        source = None
        for alias in make_name_aliases(name):
            source = alias_map.get((alias, ftype))
            if source is not None and source is not rec:
                break
        if source is None or source is rec:
            continue
        for key in metrics:
            rec[key] = source.get(key)
        if rec.get("rank_source") is None:
            rec["rank_source"] = "name_alias"
        filled += 1
    return filled


def build_basic_rows(fund_df, rank_dfs: Sequence[Tuple[str, object]]) -> List[Tuple[str, Optional[str], Optional[str], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[int], Optional[int], Optional[int], Optional[int], Optional[int], Optional[int], Optional[str], Optional[str], Optional[str]]]:
    try:
        import pandas as pd  # type: ignore
    except ModuleNotFoundError:
        print("Missing dependency: pandas", file=sys.stderr)
        print("Install: python -m pip install -r script/requirements.txt", file=sys.stderr)
        raise

    records: Dict[str, Dict[str, object]] = {}
    if fund_df is not None and not getattr(fund_df, "empty", False):
        for _, r in fund_df.iterrows():
            code = normalize_fund_code(r.get("基金代码", ""))
            if not code:
                continue
            records[code] = {
                "fund_name": None if pd.isna(r.get("基金简称")) else str(r.get("基金简称")),
                "fund_type": None if pd.isna(r.get("基金类型")) else str(r.get("基金类型")),
                "return_1m": None,
                "return_3m": None,
                "return_1y": None,
                "peer_avg_return_1m": None,
                "peer_avg_return_3m": None,
                "peer_avg_return_1y": None,
                "peer_rank_1m": None,
                "peer_rank_3m": None,
                "peer_rank_1y": None,
                "peer_count_1m": None,
                "peer_count_3m": None,
                "peer_count_1y": None,
                "peer_rank_1m_display": None,
                "peer_rank_3m_display": None,
                "peer_rank_1y_display": None,
                "rank_source": None,
            }

    source_stats: List[Tuple[str, int]] = []
    for source_name, rank_df in rank_dfs:
        ranked = prepare_rank_df(rank_df, source_name)
        matched = apply_rank_metrics(records, ranked, source_name)
        source_stats.append((source_name, matched))

    alias_filled = fill_rank_metrics_by_name_alias(records)

    rows: List[Tuple[str, Optional[str], Optional[str], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[int], Optional[int], Optional[int], Optional[int], Optional[int], Optional[int], Optional[str], Optional[str], Optional[str]]] = []
    for code, rec in records.items():
        rows.append(
            (
                code,
                rec.get("fund_name"),
                rec.get("fund_type"),
                rec.get("return_1m"),
                rec.get("return_3m"),
                rec.get("return_1y"),
                rec.get("peer_avg_return_1m"),
                rec.get("peer_avg_return_3m"),
                rec.get("peer_avg_return_1y"),
                rec.get("peer_rank_1m"),
                rec.get("peer_rank_3m"),
                rec.get("peer_rank_1y"),
                rec.get("peer_count_1m"),
                rec.get("peer_count_3m"),
                rec.get("peer_count_1y"),
                rec.get("peer_rank_1m_display"),
                rec.get("peer_rank_3m_display"),
                rec.get("peer_rank_1y_display"),
            )
        )
    matched = sum(1 for row in rows if row[3] is not None or row[4] is not None or row[5] is not None)
    source_msg = ", ".join(f"{name}={count}" for name, count in source_stats)
    print(
        f"Prepared fund_basic rows: total={len(rows)}, with_return_metrics={matched}, "
        f"sources[{source_msg}], alias_fill={alias_filled}"
    )
    return rows


def upsert_fund_basic(
    conn,
    schema: str,
    rows: Sequence[
        Tuple[
            str,
            Optional[str],
            Optional[str],
            Optional[float],
            Optional[float],
            Optional[float],
            Optional[float],
            Optional[float],
            Optional[float],
            Optional[int],
            Optional[int],
            Optional[int],
            Optional[int],
            Optional[int],
            Optional[int],
            Optional[str],
            Optional[str],
            Optional[str],
        ]
    ],
) -> int:
    from psycopg2.extras import execute_values  # type: ignore
    from psycopg2 import sql  # type: ignore

    sql_text = sql.SQL("""
        INSERT INTO {}.fund_basic (
            fund_code,
            fund_name,
            fund_type,
            return_1m,
            return_3m,
            return_1y,
            peer_avg_return_1m,
            peer_avg_return_3m,
            peer_avg_return_1y,
            peer_rank_1m,
            peer_rank_3m,
            peer_rank_1y,
            peer_count_1m,
            peer_count_3m,
            peer_count_1y,
            peer_rank_1m_display,
            peer_rank_3m_display,
            peer_rank_1y_display
        )
        VALUES %s
        ON CONFLICT (fund_code) DO UPDATE
        SET fund_name = EXCLUDED.fund_name,
            fund_type = EXCLUDED.fund_type,
            return_1m = EXCLUDED.return_1m,
            return_3m = EXCLUDED.return_3m,
            return_1y = EXCLUDED.return_1y,
            peer_avg_return_1m = EXCLUDED.peer_avg_return_1m,
            peer_avg_return_3m = EXCLUDED.peer_avg_return_3m,
            peer_avg_return_1y = EXCLUDED.peer_avg_return_1y,
            peer_rank_1m = EXCLUDED.peer_rank_1m,
            peer_rank_3m = EXCLUDED.peer_rank_3m,
            peer_rank_1y = EXCLUDED.peer_rank_1y,
            peer_count_1m = EXCLUDED.peer_count_1m,
            peer_count_3m = EXCLUDED.peer_count_3m,
            peer_count_1y = EXCLUDED.peer_count_1y,
            peer_rank_1m_display = EXCLUDED.peer_rank_1m_display,
            peer_rank_3m_display = EXCLUDED.peer_rank_3m_display,
            peer_rank_1y_display = EXCLUDED.peer_rank_1y_display,
            updated_at = NOW()
    """).format(sql.Identifier(schema)).as_string(conn)
    with conn.cursor() as cur:
        execute_values(cur, sql_text, rows, page_size=2000)
    return len(rows)


def get_latest_nav_date(conn, schema: str, fund_code: str) -> Optional[date]:
    from psycopg2 import sql  # type: ignore

    with conn.cursor() as cur:
        query = sql.SQL("SELECT MAX(nav_date) FROM {}.fund_nav WHERE fund_code = %s").format(
            sql.Identifier(schema)
        )
        cur.execute(query, (fund_code,))
        r = cur.fetchone()
        return r[0] if r and r[0] else None


def fetch_fund_nav(symbol: str):
    try:
        import akshare as ak  # type: ignore
    except ModuleNotFoundError:
        print("Missing dependency: akshare", file=sys.stderr)
        print("Install: python -m pip install -r script/requirements.txt", file=sys.stderr)
        raise

    return ak.fund_open_fund_info_em(symbol=symbol, indicator="单位净值走势")


def to_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        s = str(value).strip()
        if not s:
            return None
        if s.endswith("%"):
            s = s[:-1]
        return float(s)
    except Exception:
        return None


def build_nav_rows(df, fund_code: str, start: Optional[date], end: Optional[date], after: Optional[date]):
    try:
        import pandas as pd  # type: ignore
    except ModuleNotFoundError:
        print("Missing dependency: pandas", file=sys.stderr)
        print("Install: python -m pip install -r script/requirements.txt", file=sys.stderr)
        raise

    out: List[Tuple[str, date, Optional[float], Optional[float]]] = []
    if df is None or getattr(df, "empty", False):
        return out

    cols = list(df.columns)
    date_col = None
    nav_col = None
    growth_col = None
    for c in cols:
        if "净值日期" in str(c) or str(c).lower() in ("date", "nav_date"):
            date_col = c
        if "单位净值" in str(c) or str(c).lower() in ("unit_nav", "nav"):
            nav_col = c
        if "日增长率" in str(c) or "增长率" in str(c) or str(c).lower() in ("daily_growth_rate",):
            growth_col = c

    if date_col is None:
        return out

    for _, row in df.iterrows():
        d = row.get(date_col)
        if pd.isna(d):
            continue
        if isinstance(d, datetime):
            nav_date = d.date()
        elif isinstance(d, date):
            nav_date = d
        else:
            nav_date = parse_date(str(d))

        if after and nav_date <= after:
            continue
        if start and nav_date < start:
            continue
        if end and nav_date > end:
            continue

        unit_nav = to_float(row.get(nav_col)) if nav_col is not None else None
        daily_growth = to_float(row.get(growth_col)) if growth_col is not None else None
        out.append((fund_code, nav_date, unit_nav, daily_growth))

    return out


def upsert_fund_nav(conn, schema: str, rows: Sequence[Tuple[str, date, Optional[float], Optional[float]]]) -> int:
    from psycopg2.extras import execute_values  # type: ignore
    from psycopg2 import sql  # type: ignore

    if not rows:
        return 0

    sql_text = sql.SQL("""
        INSERT INTO {schema}.fund_nav (fund_code, nav_date, unit_nav, daily_growth_rate)
        VALUES %s
        ON CONFLICT (fund_code, nav_date) DO UPDATE
        SET unit_nav = EXCLUDED.unit_nav,
            daily_growth_rate = EXCLUDED.daily_growth_rate,
            updated_at = NOW()
    """).format(schema=sql.Identifier(schema)).as_string(conn)
    with conn.cursor() as cur:
        execute_values(cur, sql_text, rows, page_size=2000)
    return len(rows)


def iter_codes_from_basic(conn, schema: str) -> Iterable[str]:
    from psycopg2 import sql  # type: ignore

    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT fund_code FROM {}.fund_basic ORDER BY fund_code").format(sql.Identifier(schema)))
        for (code,) in cur.fetchall():
            yield code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default=os.getenv("PGSCHEMA", "public"))
    parser.add_argument(
        "--tasks",
        default="basic,nav",
        help="Comma list: basic,nav",
    )
    parser.add_argument("--codes", default="", help="Comma-separated fund codes. Empty means all.")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of codes for nav sync.")
    parser.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between API calls.")
    parser.add_argument("--start-date", default="", help="Filter NAV date >= start (YYYY-MM-DD or YYYYMMDD).")
    parser.add_argument("--end-date", default="", help="Filter NAV date <= end (YYYY-MM-DD or YYYYMMDD).")
    return parser.parse_args()


def main() -> int:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    args = parse_args()

    tasks = [t.strip().lower() for t in args.tasks.split(",") if t.strip()]
    start = parse_date(args.start_date) if args.start_date.strip() else None
    end = parse_date(args.end_date) if args.end_date.strip() else None

    conn = connect_pg()
    try:
        conn.autocommit = False
        ensure_tables(conn, args.schema)

        if "basic" in tasks:
            df = fetch_fund_basic()
            rank_dfs = [
                ("open", fetch_fund_rank()),
                ("money", fetch_fund_money_rank()),
                ("exchange", fetch_fund_exchange_rank()),
            ]
            rows = build_basic_rows(df, rank_dfs)
            inserted = upsert_fund_basic(conn, args.schema, rows)
            print("Synced fund_basic:", inserted)

        if "nav" in tasks:
            if args.codes.strip():
                codes = parse_codes(args.codes)
            else:
                codes = list(iter_codes_from_basic(conn, args.schema))

            if args.limit and args.limit > 0:
                codes = codes[: args.limit]

            total = 0
            for idx, code in enumerate(codes, start=1):
                latest = get_latest_nav_date(conn, args.schema, code)
                df = fetch_fund_nav(code)
                nav_rows = build_nav_rows(df, code, start=start, end=end, after=latest)
                n = upsert_fund_nav(conn, args.schema, nav_rows)
                total += n
                conn.commit()
                print(f"[{idx}/{len(codes)}] {code}: +{n} rows (latest={latest})")
                if args.sleep > 0:
                    time.sleep(args.sleep)

            print("Synced fund_nav rows:", total)

        conn.commit()
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
