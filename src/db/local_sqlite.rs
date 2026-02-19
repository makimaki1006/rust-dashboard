use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params_from_iter;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

/// ローカルSQLiteコネクションプール
#[derive(Clone)]
pub struct LocalDb {
    pool: Pool<SqliteConnectionManager>,
}

impl LocalDb {
    /// DBファイルパスからプールを作成
    pub fn new(path: &str) -> Result<Self, String> {
        if !Path::new(path).exists() {
            return Err(format!("SQLite file not found: {path}"));
        }

        let manager = SqliteConnectionManager::file(path);
        let pool = Pool::builder()
            .max_size(10)
            .build(manager)
            .map_err(|e| format!("SQLite pool creation failed: {e}"))?;

        Ok(Self { pool })
    }

    /// SQL実行 → Vec<HashMap<String, Value>>
    pub fn query(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::types::ToSql],
    ) -> Result<Vec<HashMap<String, Value>>, String> {
        let conn = self
            .pool
            .get()
            .map_err(|e| format!("SQLite connection failed: {e}"))?;

        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| format!("SQLite prepare failed: {e}"))?;

        let columns: Vec<String> = stmt
            .column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        let rows = stmt
            .query_map(params_from_iter(params.iter()), |row| {
                let mut map = HashMap::new();
                for (i, col) in columns.iter().enumerate() {
                    let val: Value = match row.get_ref(i) {
                        Ok(rusqlite::types::ValueRef::Null) => Value::Null,
                        Ok(rusqlite::types::ValueRef::Integer(n)) => Value::from(n),
                        Ok(rusqlite::types::ValueRef::Real(f)) => {
                            serde_json::Number::from_f64(f)
                                .map(Value::Number)
                                .unwrap_or(Value::Null)
                        }
                        Ok(rusqlite::types::ValueRef::Text(s)) => {
                            Value::String(String::from_utf8_lossy(s).to_string())
                        }
                        Ok(rusqlite::types::ValueRef::Blob(b)) => {
                            Value::String(format!("[blob: {} bytes]", b.len()))
                        }
                        Err(_) => Value::Null,
                    };
                    map.insert(col.clone(), val);
                }
                Ok(map)
            })
            .map_err(|e| format!("SQLite query failed: {e}"))?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row.map_err(|e| format!("SQLite row error: {e}"))?);
        }
        Ok(results)
    }

    /// スカラー値を取得
    pub fn query_scalar<T: rusqlite::types::FromSql>(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::types::ToSql],
    ) -> Result<T, String> {
        let conn = self
            .pool
            .get()
            .map_err(|e| format!("SQLite connection failed: {e}"))?;

        conn.query_row(sql, params_from_iter(params.iter()), |row| row.get(0))
            .map_err(|e| format!("SQLite scalar query failed: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_db() -> (tempfile::NamedTempFile, LocalDb) {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();
        // テーブル作成
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch("
            CREATE TABLE test_data (
                id INTEGER PRIMARY KEY,
                name TEXT,
                score REAL,
                data BLOB
            );
            INSERT INTO test_data VALUES (1, 'Alice', 95.5, X'DEADBEEF');
            INSERT INTO test_data VALUES (2, '', 0.0, X'');
            INSERT INTO test_data VALUES (3, NULL, NULL, NULL);
        ").unwrap();
        drop(conn);
        let db = LocalDb::new(path).unwrap();
        (tmp, db)
    }

    // テスト16: DBファイル存在 → Pool作成成功
    #[test]
    fn test_db_file_exists_success() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let result = LocalDb::new(tmp.path().to_str().unwrap());
        assert!(result.is_ok());
    }

    // テスト16逆証明: 存在しない → Err
    #[test]
    fn test_db_file_not_exists() {
        let result = LocalDb::new("/nonexistent/path/db.sqlite");
        assert!(result.is_err());
    }

    // テスト17: REAL値 → Value::Number(f64)
    #[test]
    fn test_real_value_to_number() {
        let (_tmp, db) = create_test_db();
        let rows = db.query("SELECT score FROM test_data WHERE id = 1", &[]).unwrap();
        let val = rows[0].get("score").unwrap();
        assert_eq!(val.as_f64(), Some(95.5));
    }

    // テスト18: INTEGER値 → Value::Number(i64)
    #[test]
    fn test_integer_value() {
        let (_tmp, db) = create_test_db();
        let rows = db.query("SELECT id FROM test_data WHERE id = 1", &[]).unwrap();
        let val = rows[0].get("id").unwrap();
        assert_eq!(val.as_i64(), Some(1));
    }

    // テスト19: TEXT値 → Value::String, 空文字 → ""
    #[test]
    fn test_text_value_empty() {
        let (_tmp, db) = create_test_db();
        let rows = db.query("SELECT name FROM test_data WHERE id = 2", &[]).unwrap();
        let val = rows[0].get("name").unwrap();
        assert_eq!(val.as_str(), Some(""));
    }

    // テスト20: NULL値 → Value::Null
    #[test]
    fn test_null_value() {
        let (_tmp, db) = create_test_db();
        let rows = db.query("SELECT name FROM test_data WHERE id = 3", &[]).unwrap();
        let val = rows[0].get("name").unwrap();
        assert!(val.is_null());
    }

    // テスト21: BLOB値 → "[blob: N bytes]"
    #[test]
    fn test_blob_value() {
        let (_tmp, db) = create_test_db();
        let rows = db.query("SELECT data FROM test_data WHERE id = 1", &[]).unwrap();
        let val = rows[0].get("data").unwrap();
        assert_eq!(val.as_str(), Some("[blob: 4 bytes]"));
    }

    // テスト21逆証明: 0バイトBLOB
    #[test]
    fn test_blob_zero_bytes() {
        let (_tmp, db) = create_test_db();
        let rows = db.query("SELECT data FROM test_data WHERE id = 2", &[]).unwrap();
        let val = rows[0].get("data").unwrap();
        assert_eq!(val.as_str(), Some("[blob: 0 bytes]"));
    }

    // テスト22: パラメータ化クエリ (SQL injection対策)
    #[test]
    fn test_parameterized_query_safe() {
        let (_tmp, db) = create_test_db();
        let name = "'; DROP TABLE test_data; --";
        let rows = db.query(
            "SELECT id FROM test_data WHERE name = ?",
            &[&name as &dyn rusqlite::types::ToSql],
        ).unwrap();
        assert_eq!(rows.len(), 0);
        // テーブルがまだ存在することを確認
        let count: i64 = db.query_scalar("SELECT COUNT(*) FROM test_data", &[]).unwrap();
        assert_eq!(count, 3);
    }

    // テスト23: query_scalar 正常値取得
    #[test]
    fn test_query_scalar_success() {
        let (_tmp, db) = create_test_db();
        let count: i64 = db.query_scalar("SELECT COUNT(*) FROM test_data", &[]).unwrap();
        assert_eq!(count, 3);
    }

    // テスト23逆証明: 0行結果 → Err
    #[test]
    fn test_query_scalar_no_rows() {
        let (_tmp, db) = create_test_db();
        let result: Result<i64, String> = db.query_scalar(
            "SELECT id FROM test_data WHERE id = 999",
            &[],
        );
        assert!(result.is_err());
    }

    // テスト25: read-only操作テスト（INSERT可能だがテスト自体の確認）
    #[test]
    fn test_query_returns_multiple_columns() {
        let (_tmp, db) = create_test_db();
        let rows = db.query("SELECT id, name, score FROM test_data ORDER BY id", &[]).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("name").unwrap().as_str(), Some("Alice"));
    }
}
