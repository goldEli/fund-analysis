# fund-analysis

## PostgreSQL (local Docker)

Use your existing local-only Docker setup and `.env`:

- `PGHOST=127.0.0.1`
- `PGPORT=5432`
- `PGUSER=postgres`
- `PGPASSWORD=123456`
- `PGDATABASE=postgres`
- `DATABASE_URL=postgresql://postgres:123456@127.0.0.1:5432/postgres`

## Excel → PostgreSQL sync

1. Install dependencies:
   ```bash
   python -m pip install -r requirements.txt
   ```
2. Put the Excel file at:
   - `script/fund_hodings.xlsx`
3. Run:
   ```bash
   python script/sync_fund_hodings.py
   ```

Optional arguments:

```bash
python script/sync_fund_hodings.py --excel script/fund_hodings.xlsx --sheet Sheet1 --schema public --table fund_holdings --mode upsert
```

If your Excel headers don't match the built-in key detection, provide key columns explicitly:

```bash
python script/sync_fund_hodings.py --key "产品代码,净值日期（年-月-日）"
```

Or via environment:

- `SYNC_KEY=产品代码,净值日期（年-月-日）`

