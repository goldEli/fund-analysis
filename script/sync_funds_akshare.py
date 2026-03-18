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
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """
            ).format(sql.Identifier(schema))
        )
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


def fetch_fund_basic():
    try:
        import akshare as ak  # type: ignore
    except ModuleNotFoundError:
        print("Missing dependency: akshare", file=sys.stderr)
        print("Install: python -m pip install -r script/requirements.txt", file=sys.stderr)
        raise

    return ak.fund_name_em()


def upsert_fund_basic(conn, schema: str, rows: Sequence[Tuple[str, Optional[str], Optional[str]]]) -> int:
    from psycopg2.extras import execute_values  # type: ignore

    sql_text = f"""
        INSERT INTO {schema}.fund_basic (fund_code, fund_name, fund_type)
        VALUES %s
        ON CONFLICT (fund_code) DO UPDATE
        SET fund_name = EXCLUDED.fund_name,
            fund_type = EXCLUDED.fund_type,
            updated_at = NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql_text, rows, page_size=2000)
    return len(rows)


def get_latest_nav_date(conn, schema: str, fund_code: str) -> Optional[date]:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT MAX(nav_date) FROM {schema}.fund_nav WHERE fund_code = %s",
            (fund_code,),
        )
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

    if not rows:
        return 0

    sql_text = f"""
        INSERT INTO {schema}.fund_nav (fund_code, nav_date, unit_nav, daily_growth_rate)
        VALUES %s
        ON CONFLICT (fund_code, nav_date) DO UPDATE
        SET unit_nav = EXCLUDED.unit_nav,
            daily_growth_rate = EXCLUDED.daily_growth_rate,
            updated_at = NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql_text, rows, page_size=2000)
    return len(rows)


def iter_codes_from_basic(conn, schema: str) -> Iterable[str]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT fund_code FROM {schema}.fund_basic ORDER BY fund_code")
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
            rows: List[Tuple[str, Optional[str], Optional[str]]] = []
            for _, r in df.iterrows():
                code = str(r.get("基金代码", "")).strip()
                if not code:
                    continue
                name = r.get("基金简称")
                ftype = r.get("基金类型")
                rows.append((code, None if name is None else str(name), None if ftype is None else str(ftype)))
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
