use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Turso HTTP APIクライアント（v2 pipeline）
#[derive(Clone)]
pub struct TursoClient {
    client: Client,
    url: String,
    auth_token: String,
}

/// v2 pipeline リクエスト
#[derive(Serialize)]
struct PipelineRequest {
    requests: Vec<PipelineReqItem>,
}

#[derive(Serialize)]
struct PipelineReqItem {
    #[serde(rename = "type")]
    req_type: String,
    stmt: Option<StmtBody>,
}

#[derive(Serialize)]
struct StmtBody {
    sql: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    args: Option<Vec<StmtArg>>,
}

#[derive(Serialize)]
struct StmtArg {
    #[serde(rename = "type")]
    arg_type: String,
    value: Value,
}

/// v2 pipeline レスポンス
#[derive(Deserialize, Debug)]
struct PipelineResponse {
    results: Option<Vec<PipelineResItem>>,
}

#[derive(Deserialize, Debug)]
struct PipelineResItem {
    response: Option<PipelineResResponse>,
}

#[derive(Deserialize, Debug)]
struct PipelineResResponse {
    #[serde(rename = "type")]
    _res_type: Option<String>,
    result: Option<PipelineResult>,
}

#[derive(Deserialize, Debug)]
struct PipelineResult {
    cols: Option<Vec<PipelineCol>>,
    rows: Option<Vec<Vec<PipelineValue>>>,
}

#[derive(Deserialize, Debug)]
struct PipelineCol {
    name: Option<String>,
}

#[derive(Deserialize, Debug)]
struct PipelineValue {
    #[serde(rename = "type")]
    val_type: Option<String>,
    value: Option<Value>,
}

impl TursoClient {
    pub fn new(url: &str, auth_token: &str) -> Self {
        // Turso HTTP API v2 URLを構築
        let base_url = if url.starts_with("libsql://") {
            url.replace("libsql://", "https://")
        } else {
            url.to_string()
        };
        let api_url = format!("{}/v2/pipeline", base_url.trim_end_matches('/'));

        Self {
            client: Client::new(),
            url: api_url,
            auth_token: auth_token.to_string(),
        }
    }

    /// パラメータをTurso v2形式に変換
    fn convert_params(params: &[Value]) -> Vec<StmtArg> {
        params
            .iter()
            .map(|v| match v {
                Value::String(s) => StmtArg {
                    arg_type: "text".to_string(),
                    value: Value::String(s.clone()),
                },
                Value::Number(n) => {
                    if n.is_i64() {
                        StmtArg {
                            arg_type: "integer".to_string(),
                            value: Value::String(n.to_string()),
                        }
                    } else {
                        StmtArg {
                            arg_type: "float".to_string(),
                            value: Value::Number(n.clone()),
                        }
                    }
                }
                Value::Null => StmtArg {
                    arg_type: "null".to_string(),
                    value: Value::Null,
                },
                _ => StmtArg {
                    arg_type: "text".to_string(),
                    value: Value::String(v.to_string()),
                },
            })
            .collect()
    }

    /// SQL実行 → Vec<HashMap<String, Value>> で返す
    pub async fn query(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<HashMap<String, Value>>, String> {
        let args = if params.is_empty() {
            None
        } else {
            Some(Self::convert_params(params))
        };

        let request = PipelineRequest {
            requests: vec![
                PipelineReqItem {
                    req_type: "execute".to_string(),
                    stmt: Some(StmtBody {
                        sql: sql.to_string(),
                        args,
                    }),
                },
                PipelineReqItem {
                    req_type: "close".to_string(),
                    stmt: None,
                },
            ],
        };

        let resp = self
            .client
            .post(&self.url)
            .bearer_auth(&self.auth_token)
            .json(&request)
            .send()
            .await
            .map_err(|e| format!("Turso request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Turso HTTP {status}: {body}"));
        }

        let body_text = resp
            .text()
            .await
            .map_err(|e| format!("Turso response read failed: {e}"))?;

        let pipeline_resp: PipelineResponse = serde_json::from_str(&body_text)
            .map_err(|e| format!("Turso response parse failed: {e}"))?;

        let mut rows = Vec::new();
        if let Some(results) = pipeline_resp.results {
            if let Some(item) = results.into_iter().next() {
                if let Some(response) = item.response {
                    if let Some(result) = response.result {
                        let columns: Vec<String> = result
                            .cols
                            .unwrap_or_default()
                            .into_iter()
                            .map(|c| c.name.unwrap_or_default())
                            .collect();

                        if let Some(result_rows) = result.rows {
                            for row in result_rows {
                                let mut map = HashMap::new();
                                for (i, pv) in row.into_iter().enumerate() {
                                    if let Some(col) = columns.get(i) {
                                        // 型に応じて適切なValueに変換
                                        let val = match pv.val_type.as_deref() {
                                            Some("integer") => {
                                                if let Some(Value::String(s)) = &pv.value {
                                                    s.parse::<i64>()
                                                        .map(|n| Value::Number(n.into()))
                                                        .unwrap_or(Value::Null)
                                                } else {
                                                    pv.value.unwrap_or(Value::Null)
                                                }
                                            }
                                            Some("float") => pv.value.unwrap_or(Value::Null),
                                            Some("text") => pv.value.unwrap_or(Value::Null),
                                            Some("null") | None => Value::Null,
                                            _ => pv.value.unwrap_or(Value::Null),
                                        };
                                        map.insert(col.clone(), val);
                                    }
                                }
                                rows.push(map);
                            }
                        }
                    }
                }
            }
        }

        Ok(rows)
    }

    /// スカラー値を1つだけ取得
    pub async fn query_scalar(&self, sql: &str, params: &[Value]) -> Result<Value, String> {
        let rows = self.query(sql, params).await?;
        if let Some(first_row) = rows.into_iter().next() {
            if let Some((_key, val)) = first_row.into_iter().next() {
                return Ok(val);
            }
        }
        Ok(Value::Null)
    }

    /// 接続テスト
    pub async fn test_connection(&self) -> Result<(), String> {
        self.query_scalar("SELECT 1", &[]).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // テスト6: libsql:// → https:// URL変換
    #[test]
    fn test_url_conversion_libsql() {
        let client = TursoClient::new("libsql://mydb.turso.io", "token");
        assert_eq!(client.url, "https://mydb.turso.io/v2/pipeline");
    }

    // テスト6逆証明: https:// → 二重変換しない
    #[test]
    fn test_url_no_double_conversion() {
        let client = TursoClient::new("https://mydb.turso.io", "token");
        assert_eq!(client.url, "https://mydb.turso.io/v2/pipeline");
    }

    // テスト7: 末尾/あり → 二重スラッシュにならない
    #[test]
    fn test_url_trailing_slash() {
        let client = TursoClient::new("libsql://mydb.turso.io/", "token");
        assert_eq!(client.url, "https://mydb.turso.io/v2/pipeline");
    }

    // テスト8: SQL引数の型変換 (text)
    #[test]
    fn test_convert_params_text() {
        let params = vec![Value::String("hello".to_string())];
        let args = TursoClient::convert_params(&params);
        assert_eq!(args.len(), 1);
        assert_eq!(args[0].arg_type, "text");
        assert_eq!(args[0].value, Value::String("hello".to_string()));
    }

    // テスト8逆証明: NULL引数 → null型
    #[test]
    fn test_convert_params_null() {
        let params = vec![Value::Null];
        let args = TursoClient::convert_params(&params);
        assert_eq!(args.len(), 1);
        assert_eq!(args[0].arg_type, "null");
    }

    // テスト9: SQL引数の型変換 (integer)
    #[test]
    fn test_convert_params_integer() {
        let params = vec![serde_json::json!(42)];
        let args = TursoClient::convert_params(&params);
        assert_eq!(args.len(), 1);
        assert_eq!(args[0].arg_type, "integer");
        assert_eq!(args[0].value, Value::String("42".to_string()));
    }

    // テスト10: レスポンスJSON解析（正常）
    #[test]
    fn test_parse_pipeline_response_normal() {
        let json = r#"{"results":[{"response":{"type":"execute","result":{"cols":[{"name":"cnt"}],"rows":[[{"type":"integer","value":"5"}]]}}}]}"#;
        let resp: PipelineResponse = serde_json::from_str(json).unwrap();
        assert!(resp.results.is_some());
        let results = resp.results.unwrap();
        assert_eq!(results.len(), 1);
    }

    // テスト10逆証明: 空rows → 空Vec
    #[test]
    fn test_parse_pipeline_response_empty_rows() {
        let json = r#"{"results":[{"response":{"type":"execute","result":{"cols":[{"name":"cnt"}],"rows":[]}}}]}"#;
        let resp: PipelineResponse = serde_json::from_str(json).unwrap();
        let results = resp.results.unwrap();
        let result = results[0].response.as_ref().unwrap().result.as_ref().unwrap();
        assert_eq!(result.rows.as_ref().unwrap().len(), 0);
    }

    // テスト13: query_scalar 単一値取得 - 空入力でNull
    #[test]
    fn test_empty_url_client_creation() {
        let client = TursoClient::new("", "");
        assert_eq!(client.url, "/v2/pipeline");
        assert!(client.auth_token.is_empty());
    }

    // テスト15: convert_params float
    #[test]
    fn test_convert_params_float() {
        let params = vec![serde_json::json!(3.14)];
        let args = TursoClient::convert_params(&params);
        assert_eq!(args[0].arg_type, "float");
    }

    // テスト: convert_params boolean → text変換
    #[test]
    fn test_convert_params_boolean_fallback() {
        let params = vec![Value::Bool(true)];
        let args = TursoClient::convert_params(&params);
        assert_eq!(args[0].arg_type, "text");
    }
}
