# 求人データ 職種追加手順書

新しい職種をRustダッシュボードの求人地図（Tab 6）に追加するための完全な手順。

---

## パイプライン全体図

```
1. スクレイピング（scrape_job_openings.py）
   → data/external/job_openings/job_openings_latest.csv
       ↓
2. 分類処理（run_complete_v2_perfect.py）
   → data/classified/classified_{職種名}_{日付}.csv（72カラム）
       ↓
3. 住所クリーニング（clean_addresses.py）
   → classified CSV の access 列を更新
       ↓
4. ユニーク住所抽出
   → data/unique_addresses_all.csv
       ↓
5. ジオコーディング（国土地理院API - 手動/外部ツール）
   → data/geocoded_addresses_all.csv
       ↓
6. DB構築（build_geocoded_postings_db.py）
   → rust_dashboard/data/geocoded_postings.db
       ↓
7. Rust側設定変更 + ビルド
   → rust_dashboard.exe
```

---

## Step 1: スクレイピング

### 設定ファイル確認

`python_scripts/scrapers/config.py` の `ALL_JOB_TYPE_CODES` に対象職種が含まれているか確認。

```python
ALL_JOB_TYPE_CODES = {
    "看護師/准看護師": "ans",
    "介護職/ヘルパー": "hh",
    # ... 既存職種 ...
    "新職種名": "コード",  # ← 追加
}
```

### 実行

```bash
cd python_scripts

# 特定職種のみスクレイピング
python scrape_job_openings.py --job-types "新職種名" --all-jobs

# テスト実行（東京都のみ）
python scrape_job_openings.py --job-types "新職種名" --prefectures "東京都" --all-jobs
```

| 引数 | 説明 |
|------|------|
| `--job-types` | カンマ区切りで対象職種指定 |
| `--all-jobs` | ALL_JOB_TYPE_CODES から検索（デフォルトは12職種のみ） |
| `--prefectures` | 都道府県絞り込み（テスト用） |
| `--browser` | Playwright版にフォールバック |
| `--resume` | チェックポイントから再開 |

### 出力

`data/external/job_openings/` 配下にCSVが生成される。

---

## Step 2: 分類処理

```bash
cd python_scripts
python run_complete_v2_perfect.py --input "data/external/job_openings/対象ファイル.csv"
```

### 出力

`data/classified/classified_{職種名}_{日付}.csv`（72カラム）

---

## Step 3: 住所クリーニング

スクレイピングデータの `access` 列から交通情報・地図リンクテキストを除去。

```bash
cd python_scripts
python clean_addresses.py
```

対象: `data/classified/classified_*.csv` の全ファイル（access列を上書き更新）。

除去されるパターン:
- 「大きな地図を見る ... Google Mapsで見る」
- 「{路線名} {駅名}駅から徒歩でX分」
- 改行以降のテキスト

---

## Step 4: ユニーク住所抽出

全 classified CSV の住所列からユニークな住所を抽出。

```bash
cd python_scripts
python -c "
import pandas as pd
from pathlib import Path

classified = Path('data/classified')
all_addrs = set()
for f in classified.glob('classified_*.csv'):
    df = pd.read_csv(f, usecols=['access'], dtype=str)
    all_addrs.update(df['access'].dropna().unique())

existing = pd.read_csv('data/unique_addresses_all.csv')
existing_set = set(existing['address'].values)
new_addrs = all_addrs - existing_set
print(f'新規住所: {len(new_addrs)}件')

if new_addrs:
    new_df = pd.DataFrame({'address': list(new_addrs)})
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined.to_csv('data/unique_addresses_all.csv', index=False)
    print('unique_addresses_all.csv を更新しました')
"
```

### 出力

`data/unique_addresses_all.csv`（address列 + id列）

---

## Step 5: ジオコーディング（国土地理院API）

未ジオコード住所に対して、国土地理院の住所検索APIでジオコーディングを実行。

### API仕様

| 項目 | 値 |
|------|-----|
| エンドポイント | `https://msearch.gsi.go.jp/address-search/AddressSearch` |
| パラメータ | `?q={住所文字列}` |
| レート制限 | 0.3秒/リクエスト以上 |
| レスポンス | GeoJSON（fX=経度, fY=緯度, iConf=信頼度, iLvl=精度レベル） |

### レスポンス例

```json
[{
  "geometry": {"coordinates": [139.6917, 35.6895], "type": "Point"},
  "type": "Feature",
  "properties": {
    "addressCode": "13101",
    "title": "東京都千代田区千代田",
    "dataSource": "1",
    "iConf": "5",
    "iLvl": "7"
  }
}]
```

### 信頼度・精度

| iConf | 意味 |
|-------|------|
| 5 | 完全一致 |
| 3-4 | 部分一致 |
| 1-2 | 低信頼 |

| iLvl | 意味 |
|------|------|
| 7 | 番地レベル |
| 5-6 | 丁目・字レベル |
| 3-4 | 市区町村レベル |
| 1-2 | 都道府県レベル |

### 結果をマージ

ジオコード結果を `data/geocoded_addresses_all.csv` にマージ。
カラム: `id, address, fX, fY, iConf, iLvl`

---

## Step 6: DB構築

classified CSV + ジオコード結果を結合し、SQLite DBを構築。

```bash
cd python_scripts
python build_geocoded_postings_db.py
```

### 職種マッピング

`build_geocoded_postings_db.py` 内の `JOB_TYPE_MAPPING` に新職種のマッピングを追加。

```python
JOB_TYPE_MAPPING = {
    "看護師・准看護師": "看護師",
    "介護職・ヘルパー": "介護職",
    # ... 既存 ...
    "新職種名（CSV上の名前）": "新職種名（DB上の名前）",  # ← 追加
}
```

### 入力ファイル

| ファイル | 用途 |
|---------|------|
| `data/classified/classified_*.csv` | 求人詳細（72カラム） |
| `data/unique_addresses_all.csv` | address → id マッピング |
| `data/geocoded_addresses_all.csv` | id → (lat, lng) マッピング |

### 出力ファイル

| ファイル | 説明 |
|---------|------|
| `rust_dashboard/data/geocoded_postings.db` | SQLite DB |
| `rust_dashboard/data/geocoded_postings.db.gz` | gzip圧縮版 |

### DBスキーマ

```sql
CREATE TABLE postings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type TEXT NOT NULL,
    prefecture TEXT NOT NULL,
    municipality TEXT NOT NULL,
    facility_name TEXT,
    service_type TEXT,
    employment_type TEXT,
    salary_type TEXT,
    salary_min INTEGER,
    salary_max INTEGER,
    salary_detail TEXT,
    headline TEXT,
    job_description TEXT,
    requirements TEXT,
    benefits TEXT,
    working_hours TEXT,
    holidays TEXT,
    education_training TEXT,
    access TEXT,
    special_holidays TEXT,
    tags TEXT,
    tier3_label_short TEXT,
    exp_qual_segment TEXT,
    hol_pattern TEXT,
    wh_shift_type TEXT,
    lat REAL,
    lng REAL,
    geocode_confidence INTEGER,
    geocode_level INTEGER
);

-- インデックス
CREATE INDEX idx_postings_job_pref ON postings(job_type, prefecture);
CREATE INDEX idx_postings_coords ON postings(lat, lng);
CREATE INDEX idx_postings_job_pref_muni ON postings(job_type, prefecture, municipality);
```

---

## Step 7: Rust側設定変更

### 7-1. JOB_TYPES に追加

`src/models/job_seeker.rs`:

```rust
pub const JOB_TYPES: [&str; N] = [
    // 既存職種...
    "新職種名",  // ← 追加
];
```

配列サイズ `N` を更新すること。

### 7-2. Tursoデータ有無を判断

新職種にTurso人口データ（求職者データ）がある場合:
- `TURSO_JOB_TYPES` にも追加（配列サイズ更新）

新職種にTurso人口データがない場合:
- `TURSO_JOB_TYPES` は変更不要
- Tab 1-5, 7 では自動的に「求職者データなし」メッセージが表示される

### 7-3. セグメントマッピング

セグメントDB（`segment_summary_v2.4.db`）にデータがある場合:

`src/handlers/segment.rs` の `map_job_type_to_segment()`:

```rust
"新職種名" => Some("セグメントDB上の職種名"),
```

### 7-4. render_no_data_message 更新（任意）

`src/handlers/jobmap/render.rs` の `render_no_data_message()` 内の「現在対応済み」リストを更新。
（geocoded_postings.db にデータが入れば、この関数は呼ばれないため、厳密には不要）

---

## Step 8: ビルド・検証

### ビルド

```bash
cd rust_dashboard

# コンパイルチェック
cargo check --target-dir "C:/rust_build_cache/rust_dashboard/target"

# リリースビルド
cargo build --release --target-dir "C:/rust_build_cache/rust_dashboard/target"
```

Windows注意: 日本語パス回避のため `--target-dir` で英語パスを指定する。

### 検証項目

| # | 確認事項 | 方法 |
|---|---------|------|
| 1 | コンパイルエラーなし | `cargo check` 成功 |
| 2 | ドロップダウンに新職種表示 | ブラウザで確認 |
| 3 | 既存職種が正常動作 | 「介護職」全タブ確認 |
| 4 | 新職種 Tab 6: 地図表示 | マーカーが表示されること |
| 5 | 新職種 Tab 1-5: メッセージ表示 | 「求職者データなし」が出ること（Tursoデータなしの場合） |
| 6 | 新職種 Tab 9: セグメント | マッピング追加済みなら分析表示 |

---

## 現在の職種データ状況（2026-02-22時点）

| 職種 | geocoded_db | Turso人口 | セグメント |
|------|:-----------:|:---------:|:----------:|
| 介護職 | 60,505件 | ✅ | ✅ |
| 看護師 | 33,438件 | ✅ | ✅ |
| 保育士 | 26,162件 | ✅ | ✅ |
| 薬剤師 | 22,253件 | ❌ | ✅ |
| 調理師、調理スタッフ | 14,103件 | ✅ | ✅ |
| 理学療法士 | 12,764件 | ✅ | ✅ |
| 生活支援員 | 11,647件 | ❌ | ✅ |
| 児童指導員 | 11,480件 | ❌ | ✅ |
| 栄養士 | 6,703件 | ✅ | ✅ |
| 言語聴覚士 | 6,232件 | ❌ | ✅ |
| 生活相談員 | 4,115件 | ✅ | ✅ |
| 児童発達支援管理責任者 | 4,155件 | ❌ | ✅ |
| サービス提供責任者 | 4,036件 | ✅ | ✅ |
| サービス管理責任者 | 3,410件 | ✅ | ✅ |
| 学童支援 | 2,697件 | ✅ | ✅ |
| 幼稚園教諭 | 2,478件 | ❌ | ✅ |
| 作業療法士 | 0件 | ✅ | ✅ |
| ケアマネジャー | 0件 | ✅ | ✅ |
