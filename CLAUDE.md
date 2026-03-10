# ジョブメドレー求人分析ダッシュボード (V1) - デプロイリポジトリ

**最終更新**: 2026-03-01

---

## 🔴 V1/V2 識別ルール

**このリポジトリは V1（ジョブメドレー版）のデプロイ用です。**
**V2（ハローワーク版）のコードを絶対にpushしないでください。**

| | V1: ジョブメドレー（本リポ） | V2: ハローワーク |
|---|---|---|
| **データソース** | ジョブメドレー（求職者データ） | ハローワーク（求人データ） |
| **リポジトリ** | `makimaki1006/rust-dashboard`（本リポ） | 別リポ（新規作成予定） |
| **DB** | 3個 (job_postings + segment + geocoded) | 1個 (hellowork.db) |
| **DB環境変数** | LOCAL_DB_PATH, SEGMENT_DB_PATH, GEOCODED_DB_PATH | HELLOWORK_DB_PATH |
| **フィルタ順序** | 職種 → 都道府県 → 市区町村 | 都道府県 → 市区町村 → 産業 |
| **タブ構成** | 9タブ | 6タブ |
| **雇用形態用語** | 正職員 | 正社員 |
| **config.rs** | 3つのDBパス定義 | 1つのDBパス定義 |

### 🔴 混同禁止

| 禁止操作 | 理由 |
|---------|------|
| V2の`src/`をこのリポにコピー | DB構造・ハンドラー・テンプレートが全く異なる |
| V2の`hellowork.db`をこのリポに配置 | カラム構造が異なり起動時にクラッシュ |
| V2の`config.rs`で上書き | DBパス定義が異なる |
| V2の`templates/`で上書き | タブ構成が異なる（9タブ vs 6タブ） |

---

## デプロイ構成

| 項目 | 値 |
|------|-----|
| GitHub | `https://github.com/makimaki1006/rust-dashboard.git` |
| ホスティング | Render |
| LFS対象 | `data/geocoded_postings.db.gz` |
| DB | geocoded_postings.db (~1.1GB), job_postings_minimal.db, segment_summary.db |

## デプロイ手順（V1）

```bash
# 1. ソース更新
cp -r rust_dashboard/src/ rust-dashboard-deploy/src/
cp -r rust_dashboard/templates/ rust-dashboard-deploy/templates/
cp rust_dashboard/Cargo.toml rust-dashboard-deploy/

# 2. DB更新
gzip -k data/geocoded_postings.db
cp data/geocoded_postings.db.gz rust-dashboard-deploy/data/

# 3. push
cd rust-dashboard-deploy
git add -A && git commit -m "Update dashboard" && git push
```
