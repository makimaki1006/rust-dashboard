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

/// 職種情報（プルダウン表示 + Tursoデータ有無を一元管理）
/// 新職種追加時はここに1行追加するだけでOK
pub struct JobTypeInfo {
    pub name: &'static str,
    pub has_turso_data: bool,
}

pub const ALL_JOB_TYPES: &[JobTypeInfo] = &[
    JobTypeInfo { name: "介護職", has_turso_data: true },
    JobTypeInfo { name: "看護師", has_turso_data: true },
    JobTypeInfo { name: "保育士", has_turso_data: true },
    JobTypeInfo { name: "栄養士", has_turso_data: true },
    JobTypeInfo { name: "生活相談員", has_turso_data: true },
    JobTypeInfo { name: "理学療法士", has_turso_data: true },
    JobTypeInfo { name: "作業療法士", has_turso_data: true },
    JobTypeInfo { name: "ケアマネジャー", has_turso_data: true },
    JobTypeInfo { name: "サービス管理責任者", has_turso_data: true },
    JobTypeInfo { name: "サービス提供責任者", has_turso_data: true },
    JobTypeInfo { name: "学童支援", has_turso_data: true },
    JobTypeInfo { name: "調理師、調理スタッフ", has_turso_data: true },
    JobTypeInfo { name: "薬剤師", has_turso_data: false },
    JobTypeInfo { name: "言語聴覚士", has_turso_data: false },
    JobTypeInfo { name: "児童発達支援管理責任者", has_turso_data: true },
    JobTypeInfo { name: "幼稚園教諭", has_turso_data: false },
    JobTypeInfo { name: "児童指導員", has_turso_data: false },
    JobTypeInfo { name: "生活支援員", has_turso_data: false },
    JobTypeInfo { name: "臨床検査技師", has_turso_data: true },
];

/// プルダウン用の職種名リストを取得
pub fn job_type_names() -> Vec<&'static str> {
    ALL_JOB_TYPES.iter().map(|j| j.name).collect()
}

/// 指定職種にTurso人口データがあるかチェック
pub fn has_turso_data(job_type: &str) -> bool {
    ALL_JOB_TYPES.iter().any(|j| j.name == job_type && j.has_turso_data)
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
