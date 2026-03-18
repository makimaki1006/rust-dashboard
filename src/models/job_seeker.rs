use serde::{Deserialize, Serialize};

/// 求職者データの基本構造体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSeekerRow {
    pub row_type: String,
    pub job_type: String,
    pub prefecture: String,
    pub municipality: String,
    pub label: Option<String>,
    pub value: Option<f64>,
    pub sub_label: Option<String>,
    pub sub_value: Option<f64>,
    pub extra_json: Option<String>,
}

/// 都道府県の順序（JIS 北→南）
pub const PREFECTURE_ORDER: [&str; 47] = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
];

/// サポートされる職種一覧
pub const JOB_TYPES: [&str; 19] = [
    "介護職",
    "看護師",
    "保育士",
    "栄養士",
    "生活相談員",
    "理学療法士",
    "作業療法士",
    "ケアマネジャー",
    "サービス管理責任者",
    "サービス提供責任者",
    "学童支援",
    "調理師、調理スタッフ",
    "薬剤師",
    "言語聴覚士",
    "児童発達支援管理責任者",
    "幼稚園教諭",
    "児童指導員",
    "生活支援員",
    "臨床検査技師",
];

/// Turso人口データが存在する職種（16職種）
pub const TURSO_JOB_TYPES: [&str; 16] = [
    "介護職", "看護師", "保育士", "栄養士",
    "生活相談員", "理学療法士", "作業療法士", "ケアマネジャー",
    "サービス管理責任者", "サービス提供責任者", "学童支援", "調理師、調理スタッフ",
    "児童指導員", "生活支援員", "児童発達支援管理責任者", "臨床検査技師",
];

/// 指定職種にTurso人口データがあるかチェック
pub fn has_turso_data(job_type: &str) -> bool {
    TURSO_JOB_TYPES.contains(&job_type)
}

/// Tursoデータなし職種向けのメッセージHTML
pub fn render_no_turso_data(job_type: &str, tab_name: &str) -> String {
    crate::handlers::render_empty_state(
        "求職者データなし",
        &format!(
            "「{}」の{}データは現在準備中です。求人地図タブでは求人情報を確認できます。",
            job_type, tab_name
        ),
    )
}
