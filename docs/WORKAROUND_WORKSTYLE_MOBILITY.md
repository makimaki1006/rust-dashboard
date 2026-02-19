# WORKAROUND: WORKSTYLE_MOBILITY ヒートマップ重複データ修正

**日付**: 2026-02-19
**状態**: 暫定対応（アプリ側ワークアラウンド）

---

## 問題

Workstyleタブのヒートマップ（雇用形態×移動パターン）で、
近距離・遠距離など**全てのmobility_typeで同じ値が表示される**バグ。

## 根本原因

`python_scripts/generate_mapcomplete_complete_sheets.py` の WORKSTYLE_MOBILITY 生成ロジック（旧1142-1165行）で、
`pref_flow`（ResidenceFlowデータ）が細粒度の行（居住地×勤務先×年齢×性別...）を持つにもかかわらず、
直接`iterrows()`でループしていた。

これにより2つの問題が発生:

1. **N重複**: 同一 `(municipality, workstyle, mobility_type)` の組み合わせが
   pref_flowの細粒度行数分（N回）出力される
2. **count値がmobility_typeに依存しない**: `count`は`residence_ws`（雇用形態集計）の値を
   そのまま使用しており、mobility_typeごとの差異がない

## 修正内容

### Python側（データ生成）: 修正済み・未適用

**ファイル**: `python_scripts/generate_mapcomplete_complete_sheets.py`

`pref_flow`を`groupby(['residence_municipality', 'mobility_type'])`で事前集約してから
ループするように変更。これにより問題1（N重複）は解消される。

> **注意**: CSV再生成→DB再投入は未実施。Turso無料枠の状況次第で実行予定。

### Rust側（アプリ表示）: 暫定ワークアラウンド適用中

**ファイル**: `rust_dashboard/src/handlers/workstyle.rs`

1. **SQLにmunicipality追加**: `WORKSTYLE_MOBILITY`行の重複排除にmunicipality情報が必要
2. **重複排除**: `HashMap<(municipality, workstyle, mobility_type), count>` で
   `or_insert()`により同一キーの最初の1件のみ保持（N重複のcountは全て同値なので安全）
3. **市区町村横断集約**: 重複排除後に `(workstyle, mobility_type)` でsum集約
4. **mob_map修正**: `insert()`（上書き）→ `+=`（加算）に変更

### ワークアラウンドの効果

- **重複排除**: 同一(municipality, ws, mob)のN重複 → 1件に正規化
- **差異化**: 異なる市区町村は異なるcount値を持つため、
  市区町村横断集約後はmobility_typeごとに異なる値になる
- **注意**: 単一市区町村レベルでは依然として全mobility_typeが同じ値になる可能性あり
  （これは問題2に起因し、Python側のデータ生成ロジック自体の改善が必要）

## 除去条件

以下が全て完了したらワークアラウンドを除去可能（ただし残しても無害）:

1. `python_scripts/generate_mapcomplete_complete_sheets.py` の修正が適用された状態で
   全職種のCSVを再生成
2. 再生成したCSVをTursoにインポート
3. ダッシュボードで近距離・遠距離の値が異なることを確認

## 関連ファイル

| ファイル | 変更内容 |
|---------|---------|
| `python_scripts/generate_mapcomplete_complete_sheets.py` | pref_flow groupby集約（Python側修正） |
| `rust_dashboard/src/handlers/workstyle.rs` | 重複排除ワークアラウンド（Rust側暫定） |
| `rust_dashboard/docs/WORKAROUND_WORKSTYLE_MOBILITY.md` | 本ドキュメント |
