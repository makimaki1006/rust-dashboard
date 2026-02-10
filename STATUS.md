# Rust Dashboard - 実装状況レポート

**作成日**: 2026-02-10
**最終更新**: 2026-02-10
**ステータス**: ✅ 品質完了（ブラウザ実機テスト済み）

## 概要

NiceGUI Python版ダッシュボード（`nicegui_app/main.py` 4,466行）を
Rust + Axum + HTMX で再実装する実験的プロジェクト。

## 技術スタック

| レイヤー | 技術 |
|---------|------|
| Web FW | Axum 0.8 |
| SPA更新 | HTMX 2.0 |
| チャート | ECharts 5.5.1 |
| 地図 | Leaflet 1.9.4 |
| ローカルDB | rusqlite + r2d2 |
| リモートDB | reqwest (Turso HTTP API) |
| キャッシュ | DashMap (TTL 30分) |
| 認証 | tower-sessions + bcrypt |
| DB圧縮 | flate2 (gzip起動時解凍) |

## コード規模

| カテゴリ | ファイル数 | 行数 |
|---------|----------|------|
| Rust (.rs) | 24 | ~4,200 |
| HTML テンプレート | 13 | ~1,100 |
| JavaScript | 3 | 200 |
| **合計** | **40** | **~5,500** |

## パフォーマンス（リリースビルド）

| 指標 | 値 |
|------|-----|
| キャッシュ済み応答 | 1ms |
| デバッグビルド比 | 約200倍高速 |
| Turso初回アクセス | 2-6秒（コールドスタート） |
| バイナリサイズ | 8.4MB (LTO有効) |

## 機能実装状況

### 実装済み機能（全8タブ + 共通機能）

| 機能 | 状態 |
|------|------|
| 認証（Email + パスワード + ドメイン制限） | ✅ |
| 職種切り替え（セッション保存） | ✅ |
| 都道府県セレクタ（全タブ共通） | ✅ |
| 市区町村セレクタ（カスケード） | ✅ |
| gzip DB起動時自動解凍 | ✅ |
| タブ切り替え（HTMX） | ✅ |

### タブ別機能

| タブ | 実装済み機能 | 状態 |
|------|------------|------|
| 1: 市場概況 | KPIカード6枚 + グラフ3種 + 採用課題診断カード + **3層比較パネル** | ✅ |
| 2: 人口動態 | 性別/年齢/資格 + 緊急度×性別2軸 + 転職時期別緊急度 + ペルソナ構成比 + **言語化カード4種** | ✅ |
| 3: 人材移動 | 流入/流出/地元志向率KPI + ドーナツ + フローテーブル + 距離分位統計 + **採用圏分析カード** | ✅ |
| 4: 需給バランス | 供給棒グラフ + 競争プロファイル + テーブル | ✅ |
| 5: 働き方 | 働き方ドーナツ + 緊急度×性別 | ✅ |
| 6: 求人マップ | Leaflet求人マップ + マーカー | ✅ |
| 7: 人材マップ | 人材密度棒グラフ + テーブル | ✅ |
| 8: 競合調査 | フィルタ + テーブル + 統計 + レポート + 近辺検索 + **施設形態フィルタ** | ✅ |

### 残存差分（NiceGUI版にのみ存在）

| 機能 | タブ | 重要度 |
|------|------|--------|
| レアリティ分析（複合検索インタラクティブ） | 人材属性 | 🟡 |
| 隣接県フィルタ | 人材移動 | 🟢 |

### 視覚確認状態

| 項目 | 状態 |
|------|------|
| cargo check コンパイル | ✅ エラーなし (19 warnings: dead_code) |
| リリースビルド | ✅ 8.4MB (4分34秒) |
| curlでHTTP 200確認 | ✅ |
| ブラウザ実機テスト（全8タブ） | ✅ Playwright MCP |
| 都道府県選択テスト（東京都） | ✅ |
| 3層比較パネル表示確認 | ✅ 全国 vs 東京都 5指標 |
| 言語化カード表示確認 | ✅ ターゲット/資格/隠れた人材 |
| 採用圏分析カード表示確認 | ✅ 流出先5県 + 地元志向率 |
| 施設形態フィルタ表示確認 | ✅ 780オプション |

### 既知の軽微な問題

| 問題 | タブ | 影響度 |
|------|------|--------|
| Tab 5: 全国モードで緊急度×性別チャートが空 | 働き方 | 🟢 軽微 |
| Tab 6: タブ再選択時「Map container already initialized」 | 求人マップ | 🟢 軽微 |
| favicon.ico 404 | 共通 | 🟢 無視可 |

## ビルド・起動方法

```bash
# リリースビルド（MinGW PATH設定必須）
rust_dashboard\build_release.bat

# サーバー起動
C:\rust_build_cache\rust_dashboard\target\release\rust_dashboard.exe
# → http://localhost:9216
```

## データ

| データ | パス | サイズ |
|-------|------|--------|
| ローカルSQLite (gzip) | data/job_postings_minimal.db.gz | 15.9MB |
| ローカルSQLite (解凍後) | data/job_postings_minimal.db | 87.5MB |
| GeoJSON | static/geojson/*.json | 47ファイル |

起動時に `.db.gz` が存在し `.db` がない場合、自動解凍を実行。

## ファイル構成

```
rust_dashboard/
  Cargo.toml
  build_release.bat
  .cargo/config.toml
  .env
  src/
    main.rs              # サーバー起動、ルーティング、gzip解凍
    config.rs            # 環境変数
    auth/                # 認証 + セッション管理
    db/                  # DB接続 (Turso + SQLite + キャッシュ)
    handlers/            # 8タブ + API
    models/              # データ構造体
    geo/                 # GeoJSON処理
  templates/             # HTMLテンプレート
  static/
    css/dashboard.css
    js/                  # app.js, charts.js, maps.js
    geojson/             # 47都道府県GeoJSON
  data/
    job_postings_minimal.db.gz  # ローカルSQLite (gzip圧縮)
```
