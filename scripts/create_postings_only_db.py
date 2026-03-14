"""
postings テーブルのみを含む軽量DBを作成する。
layer_* テーブルを除外し、GitHub Releases でホストするための
postings_only.db.gz を生成する。

使い方:
    python scripts/create_postings_only_db.py
"""
import sqlite3
import gzip
import shutil
import os

SRC = "data/geocoded_postings.db"
DST = "data/postings_only.db"
DST_GZ = DST + ".gz"


def main():
    if not os.path.exists(SRC):
        print(f"ERROR: {SRC} が見つかりません")
        return

    # 既存出力を削除
    for f in [DST, DST_GZ]:
        if os.path.exists(f):
            os.remove(f)

    print(f"元DB: {SRC} ({os.path.getsize(SRC) / 1024 / 1024:.1f} MB)")

    # postings-only DB を作成（フルバックアップ後に不要テーブルを削除）
    # isolation_level=None で自動コミットモード（VACUUM対策）
    src_conn = sqlite3.connect(SRC)
    dst_conn = sqlite3.connect(DST, isolation_level=None)
    src_conn.backup(dst_conn)
    src_conn.close()

    # layer_* テーブルをすべて削除
    cursor = dst_conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'layer_%'"
    )
    layer_tables = [row[0] for row in cursor.fetchall()]

    for table in layer_tables:
        dst_conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        print(f"  Dropped: {table}")

    # sqlite_sequence はSQLite内部テーブルのため DROP 不可 → 中身のみ削除
    try:
        dst_conn.execute("DELETE FROM sqlite_sequence")
    except sqlite3.OperationalError:
        pass

    # VACUUM で空き領域回収（autocommitモードなので問題なし）
    print("\nVACUUM 実行中...")
    dst_conn.execute("VACUUM")
    dst_conn.close()

    # 残存テーブル一覧を表示
    check_conn = sqlite3.connect(DST)
    check_cursor = check_conn.cursor()
    check_cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence'"
    )
    remaining = [row[0] for row in check_cursor.fetchall()]
    print(f"\n残存テーブル: {remaining}")
    for tbl in remaining:
        check_cursor.execute(f'SELECT COUNT(*) FROM "{tbl}"')
        count = check_cursor.fetchone()[0]
        print(f"  {tbl}: {count:,} rows")
    check_conn.close()

    dst_size_mb = os.path.getsize(DST) / 1024 / 1024
    print(f"\npostings_only.db: {dst_size_mb:.1f} MB")

    # gzip圧縮
    print("圧縮中...")
    with open(DST, "rb") as f_in:
        with gzip.open(DST_GZ, "wb", compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out, length=1024 * 1024)

    gz_size_mb = os.path.getsize(DST_GZ) / 1024 / 1024
    print(f"postings_only.db.gz: {gz_size_mb:.1f} MB")
    print(f"削除した layer テーブル数: {len(layer_tables)}")

    # 非圧縮DBを削除（.gzのみ残す）
    os.remove(DST)
    print(f"\n完了。{DST_GZ} を使用してください。")


if __name__ == "__main__":
    main()
