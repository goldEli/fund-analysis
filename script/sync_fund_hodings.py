import argparse
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


EXCEL_HEADER_MAP: Dict[str, str] = {
    "产品代码": "product_code",
    "产品名称": "product_name",
    "产品类型": "product_type",
    "最新净值": "nav",
    "净值日期（年-月-日）": "nav_date",
    "金额（元）": "amount_cny",
    "持仓收益（元）": "holding_profit_cny",
    "持仓收益（%）": "holding_profit_pct",
}


def install_requirements_command() -> str:
    repo_root = Path(__file__).resolve().parents[1]
    script_req = Path(__file__).resolve().parent / "requirements.txt"
    if script_req.exists():
        try:
            rel = script_req.relative_to(repo_root)
            return f"python -m pip install -r {rel.as_posix()}"
        except ValueError:
            return f"python -m pip install -r {script_req.as_posix()}"

    root_req = repo_root / "requirements.txt"
    if root_req.exists():
        return "python -m pip install -r requirements.txt"

    return "python -m pip install pandas openpyxl psycopg2-binary"


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


def normalize_column_name(name: str) -> str:
    v = str(name).strip().lower()
    v = re.sub(r"[^\w]+", "_", v, flags=re.UNICODE)
    v = re.sub(r"__+", "_", v)
    v = v.strip("_")
    if not v:
        v = "col"
    if not re.match(r"^[a-z_]", v):
        v = f"c_{v}"
    return v


def dedupe_names(names: List[str]) -> List[str]:
    used: Dict[str, int] = {}
    out: List[str] = []
    for n in names:
        base = n
        idx = used.get(base, 0)
        if idx == 0:
            used[base] = 1
            out.append(base)
            continue
        while True:
            candidate = f"{base}_{idx + 1}"
            if candidate not in used:
                used[base] = idx + 1
                used[candidate] = 1
                out.append(candidate)
                break
            idx += 1
    return out


def choose_key_columns(columns: List[str]) -> List[str]:
    cols = {c.lower() for c in columns}
    candidates = [
        ["fund_code", "stock_code", "report_date"],
        ["fund_code", "stock_code", "date"],
        ["fund_code", "security_code", "report_date"],
        ["fund_code", "security_code", "date"],
        ["fund_code", "stock_code"],
        ["fund_code", "security_code"],
        ["fund_id", "stock_code", "report_date"],
        ["fund_id", "stock_code"],
        ["id"],
    ]
    for cand in candidates:
        if all(c in cols for c in cand):
            return cand

    def find_first(patterns: List[str]) -> Optional[str]:
        for col in columns:
            for p in patterns:
                if re.search(p, col, flags=re.IGNORECASE):
                    return col
        return None

    code_col = find_first([r"fund_code\b", r"product_code\b", r"\bcode\b", r"代码"])
    date_col = find_first([r"report_date\b", r"nav_date\b", r"\bdate\b", r"日期"])
    if code_col and date_col:
        return [code_col, date_col]
    return []


def dtype_to_pg(dtype) -> str:
    try:
        import pandas as pd

        if pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        if pd.api.types.is_integer_dtype(dtype):
            return "BIGINT"
        if pd.api.types.is_float_dtype(dtype):
            return "DOUBLE PRECISION"
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "TIMESTAMP"
        return "TEXT"
    except Exception:
        return "TEXT"


def read_excel_to_dataframe(excel_path: Path, sheet: Optional[str]):
    try:
        import pandas as pd  # type: ignore
    except ModuleNotFoundError:
        print("Missing dependency: pandas", file=sys.stderr)
        print("Install:", install_requirements_command(), file=sys.stderr)
        raise

    try:
        df = pd.read_excel(excel_path, sheet_name=sheet if sheet is not None else 0)
    except ModuleNotFoundError as e:
        if "openpyxl" in str(e):
            print("Missing dependency: openpyxl", file=sys.stderr)
            print("Install:", install_requirements_command(), file=sys.stderr)
        raise

    df = df.copy()
    raw_columns = [str(c).strip() for c in list(df.columns)]
    mapped_columns = [EXCEL_HEADER_MAP.get(c, c) for c in raw_columns]
    df.columns = dedupe_names([normalize_column_name(c) for c in mapped_columns])
    return df


def parse_key_columns(value: Optional[str]) -> List[str]:
    if value is None:
        return []
    raw = value.strip()
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return [normalize_column_name(p) for p in parts]


def connect_pg():
    try:
        import psycopg2  # type: ignore
    except ModuleNotFoundError:
        print("Missing dependency: psycopg2-binary", file=sys.stderr)
        print("Install:", install_requirements_command(), file=sys.stderr)
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


def legacy_column_renames() -> Dict[str, str]:
    return {normalize_column_name(k): normalize_column_name(v) for k, v in EXCEL_HEADER_MAP.items()}


def maybe_rename_legacy_columns(conn, schema: str, table: str) -> None:
    from psycopg2 import sql  # type: ignore

    rename_map = legacy_column_renames()
    if not rename_map:
        return

    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        cur.execute(
            "SELECT to_regclass(%s)",
            (f"{schema}.{table}",),
        )
        exists = cur.fetchone()[0] is not None
        if not exists:
            return

        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        existing = {r[0] for r in cur.fetchall()}

        for old, new in rename_map.items():
            if old in existing and new not in existing:
                cur.execute(
                    sql.SQL("ALTER TABLE {}.{} RENAME COLUMN {} TO {}").format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        sql.Identifier(old),
                        sql.Identifier(new),
                    )
                )


def ensure_schema_and_table(conn, schema: str, table: str, df) -> List[str]:
    from psycopg2 import sql  # type: ignore

    columns = list(df.columns)
    col_defs = []
    for col in columns:
        pg_type = dtype_to_pg(df[col].dtype)
        col_defs.append(sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(pg_type)))

    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        cur.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(", ").join(col_defs),
            )
        )

        cur.execute(
            sql.SQL(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """
            ),
            (schema, table),
        )
        existing = {r[0] for r in cur.fetchall()}

        missing = [c for c in columns if c not in existing]
        for col in missing:
            pg_type = dtype_to_pg(df[col].dtype)
            cur.execute(
                sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.Identifier(col),
                    sql.SQL(pg_type),
                )
            )

    return columns


def to_rows(df, columns: List[str]) -> List[Tuple]:
    import pandas as pd  # type: ignore

    out_df = df[columns].copy()
    out_df = out_df.where(pd.notnull(out_df), None)
    return list(map(tuple, out_df.itertuples(index=False, name=None)))


def create_unique_index_if_needed(conn, schema: str, table: str, key_cols: list[str]) -> str:
    from psycopg2 import sql  # type: ignore

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT i.relname,
                   array_agg(a.attname ORDER BY k.ordinality) AS cols
            FROM pg_index x
            JOIN pg_class t ON t.oid = x.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_class i ON i.oid = x.indexrelid
            JOIN unnest(x.indkey) WITH ORDINALITY AS k(attnum, ordinality) ON TRUE
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
            WHERE n.nspname = %s
              AND t.relname = %s
              AND x.indisunique = TRUE
              AND x.indisprimary = FALSE
            GROUP BY i.relname
            """,
            (schema, table),
        )
        for index_name, cols in cur.fetchall():
            if list(cols) == list(key_cols):
                return index_name

    name = f"uq_{schema}_{table}_" + "_".join(key_cols)
    name = re.sub(r"[^\w]+", "_", name)[:60]
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} ({})").format(
                sql.Identifier(name),
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(", ").join(sql.Identifier(c) for c in key_cols),
            )
        )
    return name


def sync_dataframe(conn, schema: str, table: str, df, mode: str, key_cols: list[str]) -> int:
    from psycopg2 import sql  # type: ignore
    from psycopg2.extras import execute_values  # type: ignore

    columns = ensure_schema_and_table(conn, schema, table, df)
    rows = to_rows(df, columns)
    if not rows:
        return 0

    with conn.cursor() as cur:
        if mode == "replace":
            cur.execute(
                sql.SQL("TRUNCATE TABLE {}.{}").format(sql.Identifier(schema), sql.Identifier(table))
            )

        if mode == "upsert" and key_cols:
            create_unique_index_if_needed(conn, schema, table, key_cols)
            non_key_cols = [c for c in columns if c not in key_cols]
            set_expr = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                for c in non_key_cols
            )

            insert_sql = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {}").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(", ").join(sql.Identifier(c) for c in columns),
                sql.SQL(", ").join(sql.Identifier(c) for c in key_cols),
                set_expr if non_key_cols else sql.SQL(""),
            )
        else:
            insert_sql = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(", ").join(sql.Identifier(c) for c in columns),
            )

        if mode == "upsert" and key_cols and not [c for c in columns if c not in key_cols]:
            insert_sql = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT ({}) DO NOTHING").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(", ").join(sql.Identifier(c) for c in columns),
                sql.SQL(", ").join(sql.Identifier(c) for c in key_cols),
            )

        execute_values(cur, insert_sql.as_string(conn), rows, page_size=2000)

    return len(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--excel",
        default=str(Path(__file__).parent / "fund_hodings.xlsx"),
    )
    parser.add_argument("--sheet", default=None)
    parser.add_argument("--schema", default=os.getenv("PGSCHEMA", "public"))
    parser.add_argument("--table", default=os.getenv("PGTABLE", "fund_holdings"))
    parser.add_argument(
        "--key",
        default=os.getenv("SYNC_KEY", ""),
    )
    parser.add_argument(
        "--mode",
        choices=["upsert", "replace"],
        default=os.getenv("SYNC_MODE", "upsert"),
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")

    args = parse_args()
    excel_path = Path(args.excel).expanduser().resolve()
    if not excel_path.exists():
        print(f"Excel file not found: {excel_path}", file=sys.stderr)
        return 2

    df = read_excel_to_dataframe(excel_path, args.sheet)
    key_cols = parse_key_columns(args.key) or choose_key_columns(list(df.columns))
    if parse_key_columns(args.key):
        missing = [c for c in key_cols if c not in set(df.columns)]
        if missing:
            print("Key columns not found in Excel after normalization:", ", ".join(missing), file=sys.stderr)
            print("Available columns:", ", ".join(list(df.columns)), file=sys.stderr)
            return 2
    mode = args.mode
    if mode == "upsert" and not key_cols:
        mode = "replace"

    conn = connect_pg()
    try:
        conn.autocommit = False
        maybe_rename_legacy_columns(conn, args.schema, args.table)
        inserted = sync_dataframe(conn, args.schema, args.table, df, mode, key_cols)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(
        "Synced rows:",
        inserted,
        "| schema:",
        args.schema,
        "| table:",
        args.table,
        "| mode:",
        mode,
        "| key:",
        ",".join(key_cols) if key_cols else "(none)",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
