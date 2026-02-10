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
