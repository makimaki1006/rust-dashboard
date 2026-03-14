"""
geocoded_postings.db 内の layer_* テーブルを Turso にアップロードする。

環境変数:
    TURSO_DATABASE_URL  - Turso DB URL (例: https://xxxx-xxxx.turso.io)
    TURSO_AUTH_TOKEN     - Turso 認証トークン

使い方:
    # 環境変数を設定してから実行
    set TURSO_DATABASE_URL=https://your-db.turso.io
    set TURSO_AUTH_TOKEN=your-token
    python scripts/upload_layers_to_turso.py

    # ドライラン（実際には書き込まない）
    python scripts/upload_layers_to_turso.py --dry-run

    # 特定テーブルのみ
    python scripts/upload_layers_to_turso.py --tables layer_a_salary_stats layer_b_keywords

注意:
    このスクリプトはユーザーが手動で実行すること。
    Claude が直接実行することは禁止されています。
"""
import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.request
import urllib.error

# バッチサイズ（Turso HTTP API の制限を考慮して500行ずつ）
BATCH_SIZE = 500

# Turso HTTP API のリクエスト間隔（秒）- レート制限対策
REQUEST_INTERVAL = 0.1


def get_turso_config():
    """環境変数から Turso 接続情報を取得"""
    url = os.environ.get("TURSO_DATABASE_URL", "")
    token = os.environ.get("TURSO_AUTH_TOKEN", "")

    if not url:
        print("ERROR: TURSO_DATABASE_URL 環境変数が設定されていません")
        sys.exit(1)
    if not token:
        print("ERROR: TURSO_AUTH_TOKEN 環境変数が設定されていません")
        sys.exit(1)

    # URL正規化: libsql://xxx.turso.io → https://xxx.turso.io/v2/pipeline
    base_url = url.rstrip("/")
    if base_url.startswith("libsql://"):
        base_url = base_url.replace("libsql://", "https://", 1)
    if not base_url.startswith("https://"):
        base_url = "https://" + base_url
    if base_url.endswith("/v2/pipeline"):
        pass
    else:
        base_url = base_url + "/v2/pipeline"

    return base_url, token


def turso_execute(api_url, token, statements, dry_run=False):
    """
    Turso HTTP API でSQLを実行する。

    statements: list of {"sql": "...", "args": [...]} or {"sql": "..."}
    """
    requests_body = {
        "requests": [
            {"type": "execute", "stmt": stmt} for stmt in statements
        ] + [{"type": "close"}]
    }

    if dry_run:
        print(f"  [DRY RUN] {len(statements)} statements をスキップ")
        return True

    data = json.dumps(requests_body).encode("utf-8")
    req = urllib.request.Request(
        api_url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            # エラーチェック
            for i, r in enumerate(result.get("results", [])):
                if r.get("type") == "error":
                    err = r.get("error", {})
                    print(f"  ERROR in statement {i}: {err.get('message', 'unknown')}")
                    return False
            return True
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"  HTTP ERROR {e.code}: {body[:500]}")
        return False
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def turso_query_scalar(api_url, token, sql):
    """Turso でスカラー値を1つ取得する"""
    requests_body = {
        "requests": [
            {"type": "execute", "stmt": {"sql": sql}},
            {"type": "close"},
        ]
    }
    data = json.dumps(requests_body).encode("utf-8")
    req = urllib.request.Request(
        api_url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read().decode("utf-8"))
        rows = result["results"][0]["response"]["result"]["rows"]
        if rows:
            val = rows[0][0]
            # Turso は {"type": "integer", "value": "123"} 形式
            if isinstance(val, dict):
                return val.get("value", val)
            return val
        return None


def sqlite_value_to_turso_arg(value):
    """SQLite の値を Turso API の引数形式に変換"""
    if value is None:
        return {"type": "null"}
    elif isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    elif isinstance(value, float):
        return {"type": "float", "value": value}
    elif isinstance(value, bytes):
        import base64
        return {"type": "blob", "value": base64.b64encode(value).decode("ascii")}
    else:
        return {"type": "text", "value": str(value)}


def get_layer_tables(db_path):
    """layer_* テーブルの一覧を取得"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'layer_%' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables


def get_create_table_sql(db_path, table_name):
    """テーブルの CREATE TABLE 文を取得"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    row = cursor.fetchone()
    conn.close()
    if row:
        return row[0]
    return None


def upload_table(api_url, token, db_path, table_name, dry_run=False):
    """1テーブルを Turso にアップロード"""
    print(f"\n{'='*60}")
    print(f"テーブル: {table_name}")
    print(f"{'='*60}")

    # CREATE TABLE 文を取得
    create_sql = get_create_table_sql(db_path, table_name)
    if not create_sql:
        print(f"  ERROR: CREATE TABLE 文が取得できません")
        return False

    # ローカルの行数を取得
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
    local_count = cursor.fetchone()[0]
    print(f"  ローカル行数: {local_count:,}")

    # カラム情報を取得
    cursor.execute(f"PRAGMA table_info([{table_name}])")
    columns = [row[1] for row in cursor.fetchall()]
    print(f"  カラム: {', '.join(columns)}")

    # Step 1: DROP TABLE IF EXISTS
    print(f"  Step 1: DROP TABLE IF EXISTS...")
    ok = turso_execute(
        api_url, token,
        [{"sql": f"DROP TABLE IF EXISTS [{table_name}]"}],
        dry_run=dry_run,
    )
    if not ok:
        conn.close()
        return False
    time.sleep(REQUEST_INTERVAL)

    # Step 2: CREATE TABLE
    print(f"  Step 2: CREATE TABLE...")
    ok = turso_execute(
        api_url, token,
        [{"sql": create_sql}],
        dry_run=dry_run,
    )
    if not ok:
        conn.close()
        return False
    time.sleep(REQUEST_INTERVAL)

    # Step 3: バッチ INSERT
    # カラム数が少ないテーブルはバッチサイズを大きくする
    effective_batch = min(BATCH_SIZE, max(100, 5000 // max(len(columns), 1)))
    print(f"  Step 3: INSERT ({effective_batch}行/バッチ, {len(columns)}カラム)...")
    placeholders = ", ".join(["?"] * len(columns))
    insert_sql = f"INSERT INTO [{table_name}] ({', '.join(columns)}) VALUES ({placeholders})"

    cursor.execute(f"SELECT * FROM [{table_name}]")
    total_inserted = 0

    while True:
        rows = cursor.fetchmany(effective_batch)
        if not rows:
            break

        statements = []
        for row in rows:
            args = [sqlite_value_to_turso_arg(v) for v in row]
            statements.append({"sql": insert_sql, "args": args})

        ok = turso_execute(api_url, token, statements, dry_run=dry_run)
        if not ok:
            print(f"  ERROR: バッチ INSERT 失敗 (offset={total_inserted})")
            conn.close()
            return False

        total_inserted += len(rows)
        pct = total_inserted * 100 // local_count if local_count else 100
        print(f"    {total_inserted:,} / {local_count:,} ({pct}%)", end="\r")
        time.sleep(REQUEST_INTERVAL)

    print(f"    {total_inserted:,} / {local_count:,} 完了")
    conn.close()

    # Step 4: 行数検証
    if not dry_run:
        print(f"  Step 4: 行数検証...")
        time.sleep(REQUEST_INTERVAL)
        remote_count_raw = turso_query_scalar(
            api_url, token, f"SELECT COUNT(*) FROM [{table_name}]"
        )
        remote_count = int(remote_count_raw) if remote_count_raw else 0
        if remote_count == local_count:
            print(f"  OK: Turso={remote_count:,} == ローカル={local_count:,}")
        else:
            print(f"  MISMATCH: Turso={remote_count:,} != ローカル={local_count:,}")
            return False
    else:
        print(f"  Step 4: [DRY RUN] 検証スキップ")

    return True


def main():
    parser = argparse.ArgumentParser(
        description="layer_* テーブルを Turso にアップロード"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="実際には書き込まない（テーブル一覧と行数のみ表示）",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="アップロードするテーブル名（省略時は全 layer_* テーブル）",
    )
    parser.add_argument(
        "--db",
        default="data/geocoded_postings.db",
        help="ソースDBパス（デフォルト: data/geocoded_postings.db）",
    )
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: {args.db} が見つかりません")
        sys.exit(1)

    api_url, token = get_turso_config()

    # アップロード対象テーブルを決定
    all_layer_tables = get_layer_tables(args.db)
    if not all_layer_tables:
        print("layer_* テーブルが見つかりません")
        sys.exit(1)

    if args.tables:
        target_tables = [t for t in args.tables if t in all_layer_tables]
        missing = [t for t in args.tables if t not in all_layer_tables]
        if missing:
            print(f"WARNING: 以下のテーブルはDBに存在しません: {missing}")
    else:
        target_tables = all_layer_tables

    print(f"ソースDB: {args.db}")
    print(f"Turso URL: {api_url}")
    print(f"対象テーブル ({len(target_tables)}個):")
    for t in target_tables:
        print(f"  - {t}")

    if args.dry_run:
        print("\n*** DRY RUN モード ***\n")

    # アップロード実行
    results = {}
    start_time = time.time()
    for table in target_tables:
        ok = upload_table(api_url, token, args.db, table, dry_run=args.dry_run)
        results[table] = ok

    elapsed = time.time() - start_time

    # サマリー
    print(f"\n{'='*60}")
    print(f"アップロード結果サマリー")
    print(f"{'='*60}")
    success_count = 0
    fail_count = 0
    for table, ok in results.items():
        status = "OK" if ok else "FAILED"
        print(f"  {table}: {status}")
        if ok:
            success_count += 1
        else:
            fail_count += 1

    print(f"\n成功: {success_count} / 失敗: {fail_count} / 合計: {len(results)}")
    print(f"所要時間: {elapsed:.1f}秒")

    if fail_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
