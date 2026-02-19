# データファイル管理ガイド

## 重要: このドキュメントは必ず保持すること

求人データの更新・デプロイ時に必要な情報をまとめています。

---

## 1. データファイル構成

### Gitリポジトリに含まれるファイル（圧縮版）

| ファイル | サイズ目安 | 用途 |
|---------|----------|------|
| `data/geojson_gz/*.json.gz` (47ファイル) | 各200KB〜4MB | 都道府県別GeoJSON（地図表示） |
| `data/job_postings_minimal.db.gz` | ~28MB | 求人データDB（競合調査・セグメント分析） |
| `data/segment_summary.db.gz` | ~2.3MB | セグメント集約DB（Tier分析・タグ・テキスト特徴） |

### ローカルのみ（.gitignore除外）

| ファイル | サイズ目安 | 用途 |
|---------|----------|------|
| `static/geojson/*.json` (47ファイル) | 合計~300MB | 起動時にgzから自動展開 |
| `data/job_postings_minimal.db` | ~179MB | 起動時にgzから自動展開 |
| `data/segment_summary.db` | ~15MB | 起動時にgzから自動展開 |
| `data/local_competitive.db` | ~380MB | ローカル開発用（デプロイ不要） |

---

## 2. 起動時の自動展開

`src/main.rs` の起動シーケンス:
```
1. decompress_geojson_if_needed()
   data/geojson_gz/*.json.gz → static/geojson/*.json

2. decompress_db_if_needed(local_db_path)
   data/job_postings_minimal.db.gz → data/job_postings_minimal.db

3. decompress_db_if_needed(segment_db_path)
   data/segment_summary.db.gz → data/segment_summary.db
```

展開済みファイルが存在する場合はスキップされます（冪等）。

---

## 3. データ更新手順

### 求人データ（job_postings_minimal.db）を更新する場合

```bash
# 1. Pythonスクリプトで最新データを生成
cd python_scripts
python classify_all_job_types.py   # → classified CSVs
python aggregate_segments.py       # → segment_summary.db

# 2. import_job_postingsでDBを更新（別途）

# 3. 圧縮してGitリポジトリに反映
cd ../rust_dashboard
gzip -c data/job_postings_minimal.db > data/job_postings_minimal.db.gz
gzip -c data/segment_summary.db > data/segment_summary.db.gz

# 4. コミット＆プッシュ
git add data/job_postings_minimal.db.gz data/segment_summary.db.gz
git commit -m "Update job postings and segment data (YYYY-MM-DD)"
git push origin main
```

### GeoJSONデータを更新する場合

```bash
# 1. static/geojson/ に最新の47都道府県JSONを配置

# 2. 圧縮
for f in static/geojson/*.json; do
  fname=$(basename "$f")
  gzip -c "$f" > "data/geojson_gz/${fname}.gz"
done

# 3. コミット＆プッシュ
git add data/geojson_gz/
git commit -m "Update GeoJSON data"
git push origin main
```

---

## 4. Python分析スクリプト（別リポジトリ管理）

以下のPythonファイルはrust-dashboardリポジトリ外（job_medley_project/python_scripts/）に存在し、
求人分析のコア処理を担当しています。**データ更新時に必ず必要です。**

| ファイル | 用途 |
|---------|------|
| `job_medley_analyzer.py` | タグ抽出、待遇フラグ、年代推定、性別/ライフステージ推定 |
| `segment_classifier.py` | 5軸Tier2スコアリング、Tier3パターンマッチング |
| `aggregate_segments.py` | segment_summary.dbへの集約出力（v2.0） |
| `classify_all_job_types.py` | 全職種一括分類パイプライン |
| `job_posting_parser.py` | 求人詳細パーシング共通関数 |

---

## 5. デプロイ要件

| 必須ファイル | 理由 |
|-------------|------|
| `data/geojson_gz/*.json.gz` | 地図タブ（タレントマップ等）で使用 |
| `data/job_postings_minimal.db.gz` | 競合調査タブ・分析パネルで使用 |
| `data/segment_summary.db.gz` | セグメント分析タブで使用 |
| `.env.example` | 環境変数テンプレート |
| `render.yaml` | Renderデプロイ設定 |

**これらのファイルが欠落するとデプロイ後にデータが表示されません。**

---

## 6. 環境変数

`.env.example` 参照:
```
PORT=9216
TURSO_DATABASE_URL=libsql://your-db.turso.io
TURSO_AUTH_TOKEN=your-token
AUTH_PASSWORD=your-password
ALLOWED_DOMAINS=f-a-c.co.jp,cyxen.co.jp
LOCAL_DB_PATH=data/job_postings_minimal.db
CACHE_TTL_SECS=1800
RUST_LOG=info
```
