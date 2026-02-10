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
pub const JOB_TYPES: [&str; 12] = [
    "介護職",
    "看護師",
    "保育士",
    "歯科衛生士",
    "理学療法士",
    "作業療法士",
    "言語聴覚士",
    "柔道整復師",
    "あん摩マッサージ指圧師",
    "鍼灸師",
    "管理栄養士",
    "栄養士",
];
