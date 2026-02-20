# -*- coding: utf-8 -*-
"""
求人データ属性抽出エンジン

スクレイピング済みCSV(27列)から segment_classifier.py が
必要とする構造化属性を抽出する。

入力: 生CSV (募集職種, 仕事内容, 応募要件, 待遇, ... 27列)
出力: 属性追加済みDataFrame (元27列 + 抽出属性 ~30列)
"""

import re
import pandas as pd
import numpy as np
from job_posting_parser import (
    normalize_number_text,
    parse_annual_holidays,
    clean_employment_type,
    parse_access,
)


# ============================================================
# タグ定義 (49キーワード)
# ============================================================

DIRECT_TAGS = [
    "未経験可", "無資格可", "資格不問", "学歴不問",
    "新卒可", "ブランク可", "復職支援",
    "40代活躍", "50代活躍", "60代活躍", "年齢不問",
    "主夫・主婦OK", "フリーターOK", "副業OK", "WワークOK",
    "日勤のみ可", "夜勤専従", "残業ほぼなし", "残業なし",
    "4週8休以上", "土日祝休み",
    "即日勤務OK", "オープン3年以内",
    "退職金あり", "資格取得支援", "研修制度あり",
    "育児支援あり", "家庭都合休OK",
    # v1.2追加
    "テレワーク可", "フレックスタイム", "転勤なし", "駅近",
    "オープニング", "高給与", "正社員登用あり", "Web面接可",
    "産休育休実績あり", "有給取得率高", "見学可", "ICT活用",
    # v2.3追加
    "完全週休2日", "土日休み", "20代活躍", "30代活躍", "週休3日", "扶養内OK",
]

TAG_SYNONYMS = {
    "未経験可": ["経験不問", "未経験OK", "未経験ok", "未経験歓迎", "未経験者歓迎"],
    "無資格可": ["資格なしOK", "無資格OK"],
    "ブランク可": ["ブランクOK", "ブランクok"],
    "主夫・主婦OK": ["主婦OK", "主夫OK", "主婦歓迎", "主夫歓迎", "主夫・主婦OK"],
    "フリーターOK": ["フリーター歓迎"],
    "WワークOK": ["ダブルワークOK", "Wワーク可"],
    "副業OK": ["副業可"],
    "残業ほぼなし": ["残業少なめ", "残業月10時間以内"],
    "退職金あり": ["退職金制度"],
    "育児支援あり": ["育児支援", "託児所あり"],
    # v1.2追加
    "テレワーク可": ["リモートワーク可", "在宅勤務可", "在宅勤務OK"],
    "フレックスタイム": ["フレックス制", "時差出勤"],
    "転勤なし": ["転勤無し", "転勤ありません"],
    "駅近": ["駅チカ", "駅前", "駅直結"],
    "オープニング": ["オープニングスタッフ", "新規オープン"],
    "高給与": ["高収入", "高年収", "高時給"],
    "正社員登用あり": ["正社員登用制度", "社員登用あり"],
    "Web面接可": ["オンライン面接", "リモート面接", "Zoom面接"],
    "産休育休実績あり": ["育休取得実績", "産休実績あり"],
    "有給取得率高": ["有給消化率高"],
    "見学可": ["職場見学OK", "見学歓迎", "見学会"],
    "ICT活用": ["ICT導入", "DX推進"],
    # v2.3追加
    "完全週休2日": ["完全週休二日", "完全週休2日制"],
    "土日休み": ["土日お休み", "土日祝お休み", "土日祝休み"],
    "20代活躍": ["20代活躍中", "20代が活躍"],
    "30代活躍": ["30代活躍中", "30代が活躍"],
}

# v2.3: 正規表現フォールバック（部分文字列マッチ失敗時に使用）
_RE_TAG_PATTERNS = {
    "テレワーク可": re.compile(r'テレワーク|リモート(ワーク|勤務)|在宅(勤務|ワーク)'),
    "高給与": re.compile(r'高(給|収入|年収|時給)|業界(最高|トップ|高水準)'),
    "Web面接可": re.compile(r'(Web|オンライン|リモート|Zoom|ウェブ)\s*(面接|面談)'),
    "フレックスタイム": re.compile(r'フレックス(タイム)?制?|時差出勤'),
    "駅近": re.compile(r'駅.{0,3}(近|チカ|前|直結)|駅.{0,5}徒歩[1-5]分'),
    "オープニング": re.compile(r'オープニング|新規オープン|新設.{0,3}(施設|事業所)'),
    "正社員登用あり": re.compile(r'正社員(登用|への|切替|転換)'),
    "産休育休実績あり": re.compile(r'産(休|前産後).{0,10}(実績|取得)|育休.{0,5}(実績|取得率|復帰)'),
    "有給取得率高": re.compile(r'有給.{0,5}(取得率|消化率).{0,5}(高|[7-9]\d%|100%)'),
    "見学可": re.compile(r'(職場)?(見学|体験).{0,5}(可|OK|歓迎|受付)'),
    "ICT活用": re.compile(r'ICT|DX|IT.{0,5}(化|活用|導入)|電子カルテ|ペーパーレス'),
    "扶養内OK": re.compile(r'扶養(内|範囲|控除).{0,3}(OK|可|内|勤務)'),
    "週休3日": re.compile(r'週休3日|週休三日'),
}


# ============================================================
# Benefitsフラグ定義 (25種)
# ============================================================

BENEFITS_PATTERNS = {
    "has_社会保険完備": [r"社会保険完備", r"社保完備"],
    "has_賞与": [r"賞与", r"ボーナス"],
    "has_交通費支給": [r"交通費(支給|全額)", r"通勤手当"],
    "has_退職金": [r"退職金"],
    "has_住宅手当": [r"住宅手当", r"住居手当", r"家賃補助"],
    "has_資格手当": [r"資格手当"],
    "has_夜勤手当": [r"夜勤手当"],
    "has_資格取得支援": [r"資格取得(支援|補助|助成|制度)"],
    "has_研修制度": [r"研修(制度|充実|あり)"],
    "has_育児支援": [r"育児(支援|休暇|休業)", r"託児", r"保育"],
    "has_車通勤可": [r"車通勤(可|OK)", r"マイカー(通勤)?可"],
    "has_制服貸与": [r"制服(貸与|あり|支給)"],
    "has_食事補助": [r"食事補助", r"社員食堂"],
    "has_健康診断": [r"(健康|定期)診断"],
    "has_インフルエンザ補助": [r"インフルエンザ.{0,3}(補助|予防接種)"],
    "has_お祝い金": [r"お祝い金", r"入社祝"],
    "has_永年勤続": [r"永年勤続"],
    "has_持株会": [r"持株会"],
    "has_福利厚生サービス": [r"ベネフィットステーション", r"リロクラブ", r"福利厚生サービス"],
    # v1.2追加 (6種)
    "has_テレワーク": [r"テレワーク", r"リモートワーク", r"在宅勤務"],
    "has_フレックス": [r"フレックス(タイム)?", r"時差出勤"],
    "has_残業文化なし": [r"残業(ほぼ|ほとんど)?なし", r"残業.{0,3}ゼロ", r"定時(退社|帰り)"],
    "has_社員割引": [r"社員割引", r"従業員割引", r"職員割引"],
    "has_配偶者手当": [r"配偶者手当", r"扶養手当"],
    "has_子ども手当": [r"子ども手当", r"家族手当", r"子供手当"],
}


# ============================================================
# 属性抽出関数
# ============================================================

def extract_required_experience_years(text: str) -> int:
    """応募要件から必要経験年数を抽出"""
    if not isinstance(text, str):
        return 0
    text = normalize_number_text(text)

    if re.search(r"(経験不問|経験問わず|未経験可|未経験OK|未経験歓迎)", text):
        return 0

    patterns = [
        r"(?:実務|臨床|業務|介護|看護)?経験\s*(\d+)\s*年\s*(?:以上|程度)?",
        r"経験が?\s*(\d+)\s*年",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return int(m.group(1))

    return 0


def extract_age_limit(text: str) -> float:
    """応募要件から年齢上限を抽出"""
    if not isinstance(text, str):
        return np.nan
    text = normalize_number_text(text)

    patterns = [
        r"(\d{2})歳\s*(?:以下|まで|未満)",
        r"定年\s*(\d{2})歳",
        r"(\d{2})歳\s*定年",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return int(m.group(1))

    return np.nan


def extract_bonus_count(taigu_text: str, bikou_text: str) -> float:
    """賞与月数を抽出"""
    combined = ""
    if isinstance(taigu_text, str):
        combined += taigu_text + "\n"
    if isinstance(bikou_text, str):
        combined += bikou_text
    if not combined.strip():
        return 0.0

    text = normalize_number_text(combined)

    m = re.search(r"賞与.*?(\d+\.?\d*)\s*[ヶヵか箇ケ]?\s*月", text)
    if m:
        return float(m.group(1))

    if re.search(r"(賞与あり|ボーナス・賞与あり|ボーナスあり)", text):
        return 1.0

    return 0.0


def extract_annual_holidays_num(kyujitsu_text: str) -> float:
    """年間休日数を数値で返す"""
    result = parse_annual_holidays(kyujitsu_text)
    if result:
        try:
            return float(result)
        except ValueError:
            pass
    return np.nan


def extract_tags(row: pd.Series) -> str:
    """1行からタグを抽出しカンマ区切りで返す"""
    fields = ['requirements', 'welcome_requirements', 'benefits',
              'working_hours', 'holidays', 'job_description',
              'headline', 'education_training', 'staff_composition',
              'access', 'special_holidays', 'selection_process', 'salary_detail']
    combined = ""
    for f in fields:
        v = row.get(f)
        if isinstance(v, str):
            combined += " " + v

    if not combined.strip():
        return ""

    found = []

    for tag in DIRECT_TAGS:
        if tag in combined:
            found.append(tag)
        # v2.3: 正規表現フォールバック（部分文字列マッチ失敗時）
        elif tag in _RE_TAG_PATTERNS and _RE_TAG_PATTERNS[tag].search(combined):
            found.append(tag)

    for canonical, synonyms in TAG_SYNONYMS.items():
        if canonical not in found:
            for syn in synonyms:
                if syn in combined:
                    found.append(canonical)
                    break

    # 年間休日120日以上の判定
    if "年間休日120日以上" not in found:
        holidays_text = str(row.get('holidays', ''))
        h_num = extract_annual_holidays_num(holidays_text)
        if not np.isnan(h_num) and h_num >= 120:
            found.append("年間休日120日以上")

    return ",".join(found)


def extract_benefits_flags(row: pd.Series) -> dict:
    """待遇+給与備考から19個のhas_*フラグを抽出"""
    combined = ""
    for f in ['benefits', 'salary_detail', 'education_training', 'job_description']:
        v = row.get(f)
        if isinstance(v, str):
            combined += " " + v

    flags = {}
    for flag_name, patterns in BENEFITS_PATTERNS.items():
        flags[flag_name] = 0
        for pat in patterns:
            if re.search(pat, combined):
                flags[flag_name] = 1
                break

    return flags


# ============================================================
# 仕事内容分析（v1.2新機能）
# ============================================================

# 業務カテゴリの定義（10種 v2.4拡張）
JOB_DESC_CATEGORIES = {
    "直接介護・看護": [
        r'身体介護', r'入浴(介助|支援)', r'食事(介助|支援)', r'排(せつ|泄)(介助|支援)',
        r'バイタル(サイン|チェック|測定)?', r'点滴', r'採血', r'注射', r'褥瘡',
        r'服薬(管理|指導|介助)',
        # v2.4追加: 看護・医療業務キーワード
        r'看護業務', r'看護師業務', r'訪問看護', r'訪問入浴',
        r'医療処置', r'医療行為', r'健康(管理|観察|チェック)',
        r'透析', r'穿刺', r'返血', r'シャント',
        r'配薬', r'与薬', r'吸引', r'経管栄養',
        r'創傷(処置|ケア)', r'ストーマ', r'カテーテル',
        r'血糖(測定|チェック)', r'インスリン',
    ],
    "間接業務": [
        r'記録(業務|作成)', r'書類(作成|業務)', r'事務(作業|業務)', r'電話対応',
        r'レセプト', r'請求(業務|事務)', r'データ入力',
        # v2.4追加
        r'受付(業務)?', r'予約(管理|受付)',
        r'在庫管理', r'物品(管理|発注)',
    ],
    "相談支援": [
        r'相談(支援|業務|対応)', r'ケアマネジメント', r'ケアプラン',
        r'アセスメント', r'退院(支援|調整)', r'生活相談',
        # v2.4追加
        r'サービス(担当者会議|調整)', r'退院(支援|調整)',
    ],
    "リハビリ": [
        r'リハビリ(テーション)?', r'機能(訓練|回復)', r'理学療法', r'作業療法',
        r'言語(聴覚|療法)', r'ADL',
    ],
    "マネジメント": [
        r'(スタッフ|職員).{0,3}(管理|マネジメント|指導)', r'シフト(管理|作成)',
        r'人(事|材)(管理|育成)', r'施設(運営|管理)', r'管理(業務|者)',
    ],
    "保育": [
        r'保育(業務|活動)', r'(乳児|幼児).{0,3}(クラス|担当)',
        r'お散歩', r'制作活動', r'連絡帳', r'園児',
    ],
    "調理": [
        r'調理(業務|補助)?', r'配(膳|食)', r'厨房', r'栄養(管理|指導)',
    ],
    # v2.4新規カテゴリ
    "健診・検診": [
        r'健(康)?診(業務)?', r'検診', r'人間ドック',
        r'心電図', r'視力検査', r'聴力検査',
        r'問診', r'保健指導',
    ],
    "美容・自費診療": [
        r'美容(施術|カウンセリング|注射|外来|皮膚)', r'脱毛',
        r'アートメイク', r'ヒアルロン酸', r'ボトックス',
        r'レーザー(治療|施術)', r'点滴(バー|ルーム)',
    ],
    "産業保健": [
        r'産業(保健|看護)', r'健康(経営|管理室)',
        r'ストレスチェック', r'衛生(管理|委員会)',
        r'メンタルヘルス', r'復職支援',
    ],
}

# v2.4: service_typeからjd_primary_taskを推定するフォールバック辞書
_SERVICE_TYPE_TO_JD = {
    r'訪問看護': '直接介護・看護',
    r'病院|クリニック|診療所|医院': '直接介護・看護',
    r'介護.{0,3}(施設|事業所)|老人ホーム|デイサービス|グループホーム': '直接介護・看護',
    r'保育(園|所)|幼稚園|認定こども園': '保育',
    r'歯科(診療所|医院|クリニック)': '間接業務',
    r'健診|検診|人間ドック': '健診・検診',
    r'美容(クリニック|皮膚科|外科)': '美容・自費診療',
    r'薬局|ドラッグストア': '間接業務',
    r'(居宅|地域包括)': '相談支援',
}

# トーン検出パターン
_TONE_CASUAL = [r'♪', r'！{2,}', r'☆', r'★', r'ぜひ', r'一緒に', r'お気軽に']
_TONE_URGENT = [r'急募', r'至急', r'すぐ(に|働)', r'今すぐ', r'欠員']
_TONE_FORMAL = [r'つきましては', r'ご応募', r'お問い合わせ', r'下記のとおり']


def analyze_job_description(row: pd.Series) -> dict:
    """仕事内容フリーテキストから業務カテゴリ・トーン・詳細度を抽出"""
    desc = row.get('job_description')
    if not isinstance(desc, str) or len(desc.strip()) < 10:
        return {
            'jd_categories': '',
            'jd_primary_task': '',
            'jd_tone': 'unknown',
            'jd_detail_level': 'low',
        }

    # 業務カテゴリ判定
    matched_cats = []
    for cat_name, patterns in JOB_DESC_CATEGORIES.items():
        for pat in patterns:
            if re.search(pat, desc):
                matched_cats.append(cat_name)
                break

    primary = matched_cats[0] if matched_cats else ''

    # v2.4: service_typeフォールバック（パターンマッチが空の場合）
    if not primary:
        svc = row.get('service_type')
        if isinstance(svc, str) and svc.strip():
            for svc_pat, jd_cat in _SERVICE_TYPE_TO_JD.items():
                if re.search(svc_pat, svc):
                    primary = jd_cat
                    matched_cats.append(jd_cat)
                    break

    # トーン分析
    casual_count = sum(1 for p in _TONE_CASUAL if re.search(p, desc))
    urgent_count = sum(1 for p in _TONE_URGENT if re.search(p, desc))
    formal_count = sum(1 for p in _TONE_FORMAL if re.search(p, desc))

    if urgent_count >= 2:
        tone = 'urgent'
    elif casual_count >= 2:
        tone = 'casual'
    elif formal_count >= 2:
        tone = 'formal'
    else:
        tone = 'neutral'

    # 詳細度判定
    desc_len = len(desc)
    if desc_len >= 500:
        detail = 'high'
    elif desc_len >= 200:
        detail = 'medium'
    else:
        detail = 'low'

    return {
        'jd_categories': ','.join(matched_cats),
        'jd_primary_task': primary,
        'jd_tone': tone,
        'jd_detail_level': detail,
    }


# ============================================================
# 勤務時間帯パーサー (v2.1)
# ============================================================

# 全角→半角変換テーブル（数字・コロン）
_FULLWIDTH_TABLE = str.maketrans(
    '０１２３４５６７８９：～',
    '0123456789:~',
)

# 時刻範囲パターン: HH:MM～HH:MM または HH:MM~HH:MM
_RE_TIME_RANGE = re.compile(r'(\d{1,2})[：:](\d{2})\s*[～~ー−―\-〜]\s*(\d{1,2})[：:](\d{2})')

# 休憩分数パターン
_RE_BREAK = re.compile(r'休憩\s*(\d{1,3})\s*分')

# 翌日表記パターン（夜勤終了時刻を示す）
_RE_NEXT_DAY = re.compile(r'翌\s*(\d{1,2})[：:](\d{2})')


def parse_working_hours(text) -> dict:
    """勤務時間テキストから勤務形態・時刻帯・休憩・残業情報を抽出する

    Args:
        text: working_hoursカラムのテキスト（自由記述、改行区切り）

    Returns:
        8項目のdict（wh_shift_type, wh_start_hour, wh_end_hour,
        wh_start_band, wh_end_band, wh_break_minutes, wh_overtime, wh_has_night）
    """
    defaults = {
        'wh_shift_type': '不明',
        'wh_start_hour': np.nan,
        'wh_end_hour': np.nan,
        'wh_start_band': '不明',
        'wh_end_band': '不明',
        'wh_break_minutes': np.nan,
        'wh_overtime': '不明',
        'wh_has_night': False,
    }

    # NaN/None/空文字チェック
    if not isinstance(text, str) or len(text.strip()) == 0:
        return defaults

    # 全角→半角変換
    text = text.translate(_FULLWIDTH_TABLE)

    # --- シフト形態判定 ---
    has_night_keyword = bool(re.search(r'夜勤', text))
    has_no_night = bool(re.search(r'夜勤\s*なし', text))
    has_night = has_night_keyword and not has_no_night

    if re.search(r'夜勤専従', text):
        shift_type = '夜勤専従'
    elif re.search(r'[3三]交替|[3三]交代', text):
        shift_type = '3交替'
    elif re.search(r'[2二]交替|[2二]交代', text):
        shift_type = '2交替'
    elif re.search(r'日勤\s*の\s*み|日勤のみ可', text) and not has_night:
        shift_type = '日勤のみ'
    elif re.search(r'シフト制', text):
        shift_type = 'シフト制'
    else:
        shift_type = None  # 後で時刻パターン数から判定

    # --- 時刻範囲抽出 ---
    # 翌日表記の時刻を記録（end_hourから除外する）
    next_day_hours = set()
    for m in _RE_NEXT_DAY.finditer(text):
        next_day_hours.add(int(m.group(1)))

    time_ranges = _RE_TIME_RANGE.findall(text)
    start_hours = []
    end_hours = []
    end_hours_day = []  # 翌日表記でない終業時刻

    for sh, sm, eh, em in time_ranges:
        s_h = int(sh)
        e_h = int(eh)
        start_hours.append(s_h)
        end_hours.append(e_h)
        # 翌日表記でない終業時刻を日勤側候補として記録
        if e_h not in next_day_hours:
            end_hours_day.append(e_h)

    wh_start_hour = np.nan
    wh_end_hour = np.nan

    if start_hours:
        wh_start_hour = min(start_hours)
    if end_hours_day:
        # 日勤側の最も遅い終業時刻を優先
        wh_end_hour = max(end_hours_day)
    elif end_hours:
        wh_end_hour = max(end_hours)

    # v2.4: 番号付き複数シフト → シフト制
    if shift_type is None:
        numbered_shifts = re.findall(r'[（(]?\s*[①-⑩\d]\s*[）)]', text)
        if len(numbered_shifts) >= 2:
            shift_type = 'シフト制'

    # v2.4: 変形労働時間制 → シフト制
    if shift_type is None and re.search(r'変形労働時間', text):
        shift_type = 'シフト制'

    # v2.4: 「～の間でX時間」パターン → シフト制
    if shift_type is None and re.search(r'\d{1,2}:\d{2}.*の間で\d時間', text):
        shift_type = 'シフト制'

    # シフト形態が未決定の場合、時刻パターン数から判定
    if shift_type is None:
        if len(time_ranges) == 1:
            shift_type = '固定時間'
        elif len(time_ranges) == 0:
            shift_type = '不明'
        elif len(time_ranges) >= 2:
            shift_type = 'シフト制'  # v2.4: 2つ以上の時間帯 → シフト制
        else:
            shift_type = '不明'

    # --- 始業帯 ---
    if not np.isnan(wh_start_hour):
        sh = wh_start_hour
        if 5 <= sh < 7:
            start_band = '早朝(5-7時)'
        elif 7 <= sh < 9:
            start_band = '朝(7-9時)'
        elif 9 <= sh < 11:
            start_band = '午前(9-11時)'
        elif sh >= 11:
            start_band = '午後(11時以降)'
        else:
            start_band = '不明'
    else:
        start_band = '不明'

    # --- 終業帯 ---
    if not np.isnan(wh_end_hour):
        eh = wh_end_hour
        if 15 <= eh < 17:
            end_band = '午後早(15-17時)'
        elif 17 <= eh < 19:
            end_band = '夕方(17-19時)'
        elif 19 <= eh < 21:
            end_band = '夜(19-21時)'
        elif eh >= 21:
            end_band = '深夜(21時以降)'
        else:
            end_band = '不明'
    else:
        end_band = '不明'

    # --- 休憩分数 ---
    break_match = _RE_BREAK.search(text)
    wh_break_minutes = int(break_match.group(1)) if break_match else np.nan

    # --- 残業状況 (v2.4拡張) ---
    if re.search(r'(残業|時間外(労働)?)\s*(なし|ゼロ|0)', text):
        overtime = '残業なし'
    elif re.search(r'(残業|時間外(労働)?)\s*(ほぼ\s*なし|少な)', text):
        overtime = '残業ほぼなし'
    elif re.search(r'(残業|時間外(労働)?)\s*月\s*(平均\s*)?\d{1,2}\s*時間', text):
        # 時間数で分類
        m_ot = re.search(r'(残業|時間外(労働)?)\s*月\s*(平均\s*)?(\d{1,2})\s*時間', text)
        if m_ot:
            ot_hours = int(m_ot.group(4))
            if ot_hours <= 5:
                overtime = '残業ほぼなし'
            elif ot_hours <= 20:
                overtime = '月20h以内'
            else:
                overtime = '残業あり'
        else:
            overtime = '月20h以内'
    elif re.search(r'残業\s*月\s*20\s*時間\s*以内', text):
        overtime = '月20h以内'
    elif re.search(r'(時間外(労働)?\s*あり|超過勤務)', text):
        overtime = '残業あり'
    elif re.search(r'時間外', text):
        overtime = '残業あり'
    else:
        overtime = '不明'

    return {
        'wh_shift_type': shift_type,
        'wh_start_hour': int(wh_start_hour) if not np.isnan(wh_start_hour) else np.nan,
        'wh_end_hour': int(wh_end_hour) if not np.isnan(wh_end_hour) else np.nan,
        'wh_start_band': start_band,
        'wh_end_band': end_band,
        'wh_break_minutes': int(wh_break_minutes) if not np.isnan(wh_break_minutes) else np.nan,
        'wh_overtime': overtime,
        'wh_has_night': has_night,
    }


# ============================================================
# 雇用形態フォールバック抽出 (v1.0)
# ============================================================

def extract_employment_from_salary_text(text):
    """給与テキストから雇用形態を抽出するフォールバック関数

    正常なパターン: 【正職員】月給 186,000円 〜 281,800円
    異常なパターン: 正職員 月給 175,000円 〜 246,000円

    classified CSVで給与_雇用形態がNaN/空の行（住所形式行等）に対し、
    給与テキストから雇用形態を復元するために使用する。
    """
    if pd.isna(text) or not str(text).strip():
        return ''
    text = str(text).strip()

    # 墨付き括弧パターン（既存の正常パターン）
    m = re.search(r'【(.+?)】', text)
    if m:
        return _normalize_employment_type(m.group(1))

    # 括弧なしパターン: 行頭の雇用形態キーワード
    patterns = [
        '正職員', '正社員', 'パート・バイト', 'パート', 'バイト',
        '契約職員', '契約社員', '業務委託', '派遣', 'アルバイト',
    ]
    for pat in patterns:
        if text.startswith(pat):
            return _normalize_employment_type(pat)

    # 途中にキーワードがある場合
    m2 = re.search(
        r'(正職員|正社員|パート・バイト|パート|契約職員|契約社員|業務委託|派遣)',
        text,
    )
    if m2:
        return _normalize_employment_type(m2.group(1))

    return ''


def _normalize_employment_type(raw):
    """雇用形態の表記ゆれを正規化"""
    raw = raw.strip()
    if raw in ('正職員', '正社員'):
        return '正社員'
    if raw in ('パート・バイト', 'パート', 'バイト', 'アルバイト'):
        return 'パート・バイト'
    if raw in ('契約職員', '契約社員'):
        return '契約職員'
    if raw == '業務委託':
        return '業務委託'
    if raw == '派遣':
        return '派遣'
    return raw


# ============================================================
# 休日パターンパーサー (v2.2)
# ============================================================

def parse_holidays(holidays_text):
    """休日テキストから休日パターンを抽出

    Returns:
        dict: {
            'hol_pattern': str,  # 4週8休/完全週休2日/週休2日/シフト制/土日祝休/日曜固定休/その他
            'hol_weekday_off': str,  # 土日/日曜/平日/不明
            'hol_special': str,  # 年末年始,夏季,GW のカンマ区切り
        }
    """
    if not isinstance(holidays_text, str) or not holidays_text.strip():
        return {'hol_pattern': '', 'hol_weekday_off': '', 'hol_special': ''}

    text = holidays_text
    # v2.4: 漢数字→算用数字の正規化
    text = text.replace('二日', '2日').replace('三日', '3日')

    # 休日パターン判定（優先順）
    pattern = 'その他'
    if re.search(r'完全週休2日', text):
        pattern = '完全週休2日'
    elif re.search(r'4週[89]休', text):
        pattern = '4週8休'
    elif re.search(r'週休2日', text):
        pattern = '週休2日'
    elif re.search(r'土日祝(休|日)', text):
        pattern = '土日祝休'
    elif re.search(r'日曜(日)?(.{0,2})(休|固定)', text):
        pattern = '日曜固定休'
    elif re.search(r'シフト制', text):
        pattern = 'シフト制'

    # v2.4: 「その他」のフォールバック追加パターン
    if pattern == 'その他':
        # 月X日休み → 4週8休換算
        if re.search(r'月[89]\d?\s*日?(以上)?\s*(の?\s*)?休', text):
            pattern = '4週8休'
        elif re.search(r'月10\s*日?(以上)?\s*(の?\s*)?休', text):
            pattern = '4週8休'
        elif re.search(r'月[67]\s*日?(以上)?\s*(の?\s*)?休', text):
            pattern = 'シフト制'
        # 勤務パターン (3勤1休, 4勤2休)
        elif re.search(r'[34]勤[12]休', text):
            pattern = '4週8休'
        # シフト関連
        elif re.search(r'シフトによる|ローテーション', text):
            pattern = 'シフト制'
        # パート応相談
        elif re.search(r'週[1-3]日.{0,5}(OK|可|勤務|から)|応相談', text):
            pattern = 'パート応相談'

    # 曜日固定休の判定
    weekday_off = '不明'
    if re.search(r'土日(祝)?(.{0,2})(休|お休み)', text):
        weekday_off = '土日'
    elif re.search(r'日曜(日)?(.{0,2})(休|固定)', text):
        weekday_off = '日曜'
    elif re.search(r'平日(.{0,2})(休|お休み)', text):
        weekday_off = '平日'
    # v2.4: クリニック系の曜日固定休
    elif re.search(r'水曜.{0,5}(休|午後休)', text):
        weekday_off = '水曜'
    elif re.search(r'木曜.{0,5}(休|午後休)', text):
        weekday_off = '木曜'

    # 特別休暇
    specials = []
    if re.search(r'年末年始', text):
        specials.append('年末年始')
    if re.search(r'(お盆|夏季休暇|夏期休暇)', text):
        specials.append('夏季')
    if re.search(r'(GW|ゴールデンウィーク)', text):
        specials.append('GW')
    if re.search(r'(慶弔|冠婚葬祭)', text):
        specials.append('慶弔')
    if re.search(r'(リフレッシュ休暇|アニバーサリー)', text):
        specials.append('リフレッシュ')

    return {
        'hol_pattern': pattern,
        'hol_weekday_off': weekday_off,
        'hol_special': ','.join(specials),
    }


# ============================================================
# 年代セグメント検出 (v2.0)
# ============================================================

# 各年代に紐づくキーワードパターン（正規表現）
# 全カラム横断検索で使用
AGE_DECADE_PATTERNS = {
    '20代': [
        r'髪(色|型).{0,3}(自由|OK)',
        r'ネイル.{0,3}(自由|OK)',
        r'ピアス.{0,3}(自由|OK)',
        r'服装.{0,3}自由',
        r'プライベート.{0,3}充実',
        r'第二新卒',
        r'新卒.{0,3}(歓迎|可|OK)',
        r'20代.{0,3}(活躍|中心|多い)',
        r'若手.{0,3}(活躍|中心|多い|歓迎)',
        r'フリーター.{0,3}(歓迎|OK|可)',
        r'友達.{0,3}(応募|と一緒)',
        r'SNS',
        r'平均年齢.{0,3}2\d歳',
    ],
    '30代': [
        r'家庭.{0,5}(両立|と両立)',
        r'子育て.{0,5}(しながら|中の方|ママ|世代)',
        r'30代.{0,3}(活躍|中心|多い)',
        r'ブランク.{0,3}(OK|可|歓迎|ある方)',
        r'時短勤務',
        r'育児.{0,3}(短時間|休暇|支援)',
        r'産休.{0,3}育休',
        r'ワークライフバランス',
        r'家庭都合.{0,3}休',
        r'ママ.{0,3}(さん|歓迎|活躍|多い)',
        r'主婦.{0,3}(歓迎|活躍|パート)',
        r'平均年齢.{0,3}3\d歳',
        r'保育(所|園).{0,5}(あり|完備|利用)',
        r'託児(所|施設)',
    ],
    '40代': [
        r'40代.{0,3}(活躍|中心|多い)',
        r'経験.{0,3}(活かせる|を活かし)',
        r'管理職',
        r'主任',
        r'リーダー',
        r'中堅',
        r'即戦力',
        r'経験者.{0,3}(優遇|歓迎|求む)',
        r'キャリアアップ',
        r'スキルアップ',
        r'昇進',
        r'平均年齢.{0,3}4\d歳',
    ],
    '50代': [
        r'50代.{0,3}(活躍|中心|多い|歓迎)',
        r'シニア.{0,3}(活躍|歓迎|層)',
        r'定年.{0,3}(後|再雇用)',
        r'再雇用',
        r'経験豊富',
        r'ベテラン.{0,3}(歓迎|活躍|多い)',
        r'熟練',
        r'長年.{0,3}(経験|実績)',
        r'平均年齢.{0,3}5\d歳',
    ],
    '60代': [
        r'60代.{0,3}(活躍|中心|多い|歓迎)',
        r'生涯現役',
        r'シニア.{0,3}歓迎',
        r'定年.{0,3}(なし|65歳|70歳|延長)',
        r'エイジレス',
        r'年齢.{0,3}(不問|上限なし)',
        r'高齢者.{0,3}(歓迎|活躍)',
    ],
}

# 検索対象カラム（全カラム横断）
_AGE_SEARCH_FIELDS = [
    'headline', 'job_description', 'requirements', 'welcome_requirements',
    'benefits', 'salary_detail', 'working_hours', 'holidays',
    'education_training', 'staff_composition', 'selection_process',
    'access', 'special_holidays',
]


def detect_age_decade(row: pd.Series) -> dict:
    """全カラム横断で年代シグナルを検出し、各年代のスコアと推定年代を返す

    Returns:
        dict with keys: age_20s_score, age_30s_score, age_40s_score,
                        age_50s_score, age_60s_score, age_decade_primary, age_decade_all
    """
    combined = ""
    for f in _AGE_SEARCH_FIELDS:
        v = row.get(f)
        if isinstance(v, str):
            combined += " " + v

    if not combined.strip():
        return {
            'age_20s_score': 0, 'age_30s_score': 0, 'age_40s_score': 0,
            'age_50s_score': 0, 'age_60s_score': 0,
            'age_decade_primary': '', 'age_decade_all': '',
        }

    scores = {}
    for decade, patterns in AGE_DECADE_PATTERNS.items():
        s = 0
        for pat in patterns:
            if re.search(pat, combined):
                s += 1
        scores[decade] = s

    # 主要年代（最高スコア、同点なら若い方を優先）
    primary = ''
    max_score = 0
    for dec in ['20代', '30代', '40代', '50代', '60代']:
        if scores.get(dec, 0) > max_score:
            max_score = scores[dec]
            primary = dec

    # スコア1以上の年代をすべてリスト
    all_decades = ','.join(d for d in ['20代', '30代', '40代', '50代', '60代'] if scores.get(d, 0) >= 1)

    return {
        'age_20s_score': scores.get('20代', 0),
        'age_30s_score': scores.get('30代', 0),
        'age_40s_score': scores.get('40代', 0),
        'age_50s_score': scores.get('50代', 0),
        'age_60s_score': scores.get('60代', 0),
        'age_decade_primary': primary,
        'age_decade_all': all_decades,
    }


# ============================================================
# 性別・女性ライフステージ検出 (v2.0)
# ============================================================

# 性別シグナルキーワード
GENDER_SIGNAL_PATTERNS = {
    'female': [
        r'女性.{0,3}(活躍|多い|中心|歓迎|スタッフ|が多|の方)',
        r'ママ.{0,3}(さん|歓迎|活躍|多い|ナース)',
        r'主婦.{0,3}(歓迎|活躍|パート|の方)',
        r'ネイル.{0,3}(自由|OK)',
        r'髪(色|型).{0,3}(自由|OK)',
        r'産(前|後|休)',
        r'育休',
        r'マタニティ',
        r'託児',
        r'保育(所|園).{0,5}(あり|完備|利用)',
        r'女性管理職',
    ],
    'male': [
        r'男性.{0,3}(活躍|多い|中心|歓迎|スタッフ)',
        r'力仕事',
        r'体力.{0,3}(必要|ある方|に自信)',
        r'男性管理職',
    ],
}

# 女性ライフステージ（ライフストーリー順）
WOMEN_LIFECYCLE_PATTERNS = {
    '新卒・キャリア初期': [
        r'新卒',
        r'第二新卒',
        r'未経験.{0,3}(歓迎|可|OK).{0,10}若手',
        r'社会人.{0,3}(1|2|3)年',
        r'入社.{0,3}(1|2|3)年',
    ],
    'キャリア形成期': [
        r'スキルアップ',
        r'キャリアアップ',
        r'正社員登用',
        r'資格取得.{0,5}(支援|費用|補助)',
        r'昇進',
        r'研修.{0,5}(充実|制度)',
        r'等級.{0,3}制度',
    ],
    '結婚・出産期': [
        r'産(前|後|休)',
        r'育(休|児休)',
        r'産前産後',
        r'出産.{0,3}(祝|手当|休)',
        r'マタニティ',
        r'結婚.{0,3}(祝|手当|休)',
    ],
    '育児期': [
        r'時短勤務',
        r'託児(所|施設)',
        r'保育(所|園).{0,5}(あり|完備|利用)',
        r'子.{0,2}(ども|供).{0,3}(手当|看護)',
        r'家庭.{0,5}(両立|と両立)',
        r'子育て',
        r'ママ.{0,3}(さん|歓迎|活躍)',
        r'学校行事.{0,3}(参加|休)',
        r'急な.{0,5}(休|対応)',
        r'扶養内',
    ],
    '復職期': [
        r'ブランク.{0,3}(OK|可|歓迎|ある方)',
        r'復職.{0,3}(支援|歓迎|OK)',
        r'再就職',
        r'主婦.{0,3}(歓迎|活躍|パート)',
        r'久しぶり.{0,5}(仕事|復帰)',
        r'現場復帰',
    ],
    'セカンドキャリア期': [
        r'管理職',
        r'主任',
        r'リーダー',
        r'経験.{0,3}(活かせる|を活かし)',
        r'キャリア.{0,3}チェンジ',
        r'転職.{0,3}(歓迎|支援)',
    ],
    '介護離職・復帰期': [
        r'介護休(暇|業)',
        r'家族.{0,3}介護',
        r'家庭都合.{0,3}休',
        r'短時間.{0,3}(勤務|パート)',
        r'柔軟.{0,5}(勤務|シフト|働)',
    ],
}


def detect_gender_lifecycle(row: pd.Series) -> dict:
    """全カラム横断で性別シグナルと女性ライフステージを検出

    Returns:
        dict with keys: gender_female_score, gender_male_score,
                        gender_signal, lifecycle_stages, lifecycle_primary
    """
    combined = ""
    for f in _AGE_SEARCH_FIELDS:
        v = row.get(f)
        if isinstance(v, str):
            combined += " " + v

    if not combined.strip():
        return {
            'gender_female_score': 0, 'gender_male_score': 0,
            'gender_signal': '', 'lifecycle_stages': '', 'lifecycle_primary': '',
        }

    # 性別スコア
    f_score = sum(1 for pat in GENDER_SIGNAL_PATTERNS['female'] if re.search(pat, combined))
    m_score = sum(1 for pat in GENDER_SIGNAL_PATTERNS['male'] if re.search(pat, combined))

    if f_score > m_score:
        gender = 'female_leaning'
    elif m_score > f_score:
        gender = 'male_leaning'
    elif f_score > 0:
        gender = 'neutral'
    else:
        gender = ''

    # ライフステージ検出
    stage_scores = {}
    for stage, patterns in WOMEN_LIFECYCLE_PATTERNS.items():
        s = sum(1 for pat in patterns if re.search(pat, combined))
        if s > 0:
            stage_scores[stage] = s

    # スコア順に並べてカンマ区切り
    sorted_stages = sorted(stage_scores.keys(), key=lambda k: stage_scores[k], reverse=True)
    stages_str = ','.join(sorted_stages)
    primary_stage = sorted_stages[0] if sorted_stages else ''

    # v2.4: lifecycleからの性別推定（genderが空の場合のフォールバック）
    if gender == '' and primary_stage in ('結婚・出産期', '育児期', '復職期'):
        gender = 'female_leaning'
        f_score += 1

    return {
        'gender_female_score': f_score,
        'gender_male_score': m_score,
        'gender_signal': gender,
        'lifecycle_stages': stages_str,
        'lifecycle_primary': primary_stage,
    }


# ============================================================
# 未経験×資格 細分化 (v2.0)
# ============================================================

def detect_experience_qualification(row: pd.Series) -> dict:
    """未経験/有経験 × 有資格/無資格の4象限分類 v2.4

    v2.4: 資格名パターン30種+拡張、短文req判定、service_type活用
    目標: 「条件不明」38% → 15%以下

    Returns:
        dict with keys: exp_qual_segment, is_inexperienced, requires_qualification
    """
    req = str(row.get('requirements', ''))
    welcome = str(row.get('welcome_requirements', ''))
    edu = str(row.get('education_training', ''))
    jd = str(row.get('job_description', ''))
    headline = str(row.get('headline', ''))
    tags_str = str(row.get('tags', ''))
    service_type = str(row.get('service_type', ''))

    # 主要判定フィールド（応募要件 + 歓迎要件）
    combined = req + ' ' + welcome
    # 補助判定フィールド（仕事内容 + 研修 + 見出し）
    extended = combined + ' ' + jd + ' ' + edu + ' ' + headline

    # ────────────────────────────────
    # 資格名マスター（30種+）
    # ────────────────────────────────
    _QUAL_NAMES = (
        r'正看護師|准看護師|看護師免許|看護師資格|看護師'
        r'|保健師|助産師|認定看護師|専門看護師'
        r'|介護福祉士|社会福祉士|精神保健福祉士'
        r'|ケアマネジャー|ケアマネ|介護支援専門員'
        r'|初任者研修|実務者研修'
        r'|理学療法士|作業療法士|言語聴覚士'
        r'|管理栄養士|栄養士|調理師'
        r'|歯科衛生士|歯科技工士'
        r'|薬剤師|登録販売者'
        r'|柔道整復師|あん摩マッサージ指圧師|鍼灸師'
        r'|臨床検査技師|診療放射線技師|臨床工学技士'
        r'|児童指導員|保育士'
        r'|社会福祉主事|相談支援専門員'
        r'|児童指導員任用資格|児童発達支援管理責任者'
        r'|サービス管理責任者|サービス提供責任者'
    )

    # ────────────────────────────────
    # 未経験判定（拡張版）
    # ────────────────────────────────
    inexperienced_patterns = [
        r'未経験.{0,3}(歓迎|可|OK|者|の方|から|でも)',
        r'経験.{0,3}(不問|不要|なし.{0,3}(可|OK)|なくても|問いません|問わず)',
        r'(新卒|第二新卒).{0,5}(歓迎|可|OK|募集|採用)',
        r'初心者.{0,3}(OK|可|歓迎|から|でも)',
        r'(一から|イチから|ゼロから|ゼロ.{0,3}スタート)',
        r'(丁寧に|しっかり|イチから).{0,5}(教え|指導|研修|サポート)',
        r'(はじめての方|初めての方).{0,3}(歓迎|OK|でも|も安心)',
        r'ブランク.{0,3}(OK|可|歓迎|ある方|の方)',
    ]
    is_inexperienced = any(re.search(p, combined) for p in inexperienced_patterns)
    # タグからの補完（extract_tags()で検出済み）
    if not is_inexperienced and '未経験可' in tags_str:
        is_inexperienced = True
    # 補助フィールドからの弱い推定（応募要件に経験条件がない場合のみ）
    if not is_inexperienced and not re.search(r'経験.{0,3}(年|以上|必須|必要|者)', combined):
        extended_inexperienced = [
            r'(丁寧に|しっかり|イチから).{0,5}(教え|指導|研修|サポート)',
            r'研修.{0,3}(充実|制度|あり|体制|プログラム)',
            r'(安心して|安心の).{0,3}(スタート|始められ|働け)',
        ]
        if any(re.search(p, extended) for p in extended_inexperienced):
            is_inexperienced = True

    # ────────────────────────────────
    # 経験者要求判定
    # ────────────────────────────────
    experienced_patterns = [
        r'(実務)?経験.{0,3}(\d+年|年以上|必須|者|ある方)',
        r'即戦力',
        r'経験者.{0,3}(歓迎|優遇|募集|のみ)',
    ]
    is_experienced = any(re.search(p, combined) for p in experienced_patterns)

    # ────────────────────────────────
    # 資格要否判定（v2.4大幅拡張）
    # ────────────────────────────────
    no_qual_patterns = [
        r'無資格.{0,3}(可|OK|歓迎|の方|でも)',
        r'資格.{0,3}(不問|なし.{0,3}可|不要|なくても|問いません|問わず)',
        r'資格.{0,3}(取得支援|取得.{0,3}(サポート|補助|制度))',
        # v2.4: 「学歴不問」は資格不問とは別概念 → 学歴を除外
        r'資格.{0,3}不問',
    ]
    # タグからの補完（v2.4: 資格名が応募要件にある場合はno_qualにしない）
    if ('無資格可' in tags_str or '資格不問' in tags_str) and not qual_name_in_req:
        no_qual_patterns.append(r'.')  # 強制マッチ

    requires_qual_patterns = [
        r'(要|必須).{0,5}(資格|免許)',
        r'(資格|免許).{0,3}(必須|要|お持ちの方|保有者|をお持ち)',
        r'以下.{0,5}(資格|免許)',
        r'いずれか.{0,5}(資格|免許)',
        # 具体的な資格名 + 必須系キーワード
        r'(' + _QUAL_NAMES + r').{0,5}(必須|必要|要|お持ちの方|以上|保有)',
    ]
    # 資格名が応募要件に明示的に記載されている場合も資格必須と推定
    qual_name_in_req = bool(re.search(r'(' + _QUAL_NAMES + r')', req))

    no_qual = any(re.search(p, combined) for p in no_qual_patterns)
    req_qual = any(re.search(p, combined) for p in requires_qual_patterns)
    # 応募要件に資格名があり、「無資格可」が明示されていない場合は資格必須と推定
    if not req_qual and not no_qual and qual_name_in_req:
        req_qual = True

    # ────────────────────────────────
    # v2.4: 短文requirements + 資格名 → 経験不問・資格必要と推定
    # 「正看護師」のような資格名のみ記述を救済
    # ────────────────────────────────
    if not is_inexperienced and not is_experienced and qual_name_in_req:
        req_stripped = req.strip()
        if len(req_stripped) <= 30:
            # 経験条件の言及がなく短文 → 経験不問（資格のみ記載）と推定
            if not re.search(r'経験.{0,3}(年|以上|必須|必要|者)', req):
                is_inexperienced = True

    # ────────────────────────────────
    # v2.4: service_typeコンテキスト活用
    # 法令上の資格要件がある施設形態から推定
    # ────────────────────────────────
    if not req_qual and not no_qual:
        if re.search(r'訪問看護', service_type):
            req_qual = True  # 訪問看護ステーション → 看護師免許必須
        elif re.search(r'保育(園|所)|幼稚園|認定こども園', service_type):
            req_qual = True  # 保育施設 → 保育士 or 看護師
        elif re.search(r'薬局|ドラッグストア', service_type):
            req_qual = True
        elif re.search(r'歯科', service_type):
            req_qual = True
        elif re.search(r'病院|クリニック|診療所|医院', service_type):
            req_qual = True  # 医療機関 → 原則資格必要

    # ────────────────────────────────
    # v2.4: req_qualがTrueの場合、no_qualを上書き
    # （資格名が明示的に要求されている場合、一般的な「学歴不問」等に負けない）
    # ────────────────────────────────
    if req_qual and no_qual:
        no_qual = False

    # ────────────────────────────────
    # 4象限分類（フォールバック付き）
    # ────────────────────────────────
    if is_inexperienced and no_qual:
        segment = '未経験・無資格OK'
    elif is_inexperienced and (req_qual or not no_qual):
        segment = '未経験歓迎・資格必要'
    elif not is_inexperienced and no_qual:
        segment = '経験者・無資格可'
    elif (not is_inexperienced and req_qual) or (is_experienced and req_qual):
        segment = '経験者・資格必須'
    elif is_experienced and not no_qual:
        # 経験者要求があり無資格OKでなければ経験者・資格必須寄り
        segment = '経験者・資格必須'
    elif not is_inexperienced and not req_qual and not no_qual:
        # どちらとも判定できない場合のフォールバック
        # 研修制度が充実 → 未経験寄り
        if len(edu) > 100 or re.search(r'研修.{0,5}(充実|あり|制度)', extended):
            segment = '未経験歓迎・資格必要'
        # 給与が低め or 簡単な仕事 → 未経験寄り
        elif re.search(r'(簡単|シンプル|単純|難しくない|誰でも)', extended):
            segment = '未経験・無資格OK'
        else:
            segment = '条件不明'
    else:
        segment = '条件不明'

    return {
        'exp_qual_segment': segment,
        'is_inexperienced': int(is_inexperienced),
        'requires_qualification': int(req_qual and not no_qual),
    }


def compute_content_richness_score(row: pd.Series) -> int:
    """掲載充実度スコア (0-10)"""
    score = 0

    def _len(field):
        v = row.get(field)
        return len(str(v)) if isinstance(v, str) else 0

    if _len('job_description') >= 200: score += 2
    if _len('benefits') >= 200: score += 1
    if _len('education_training') >= 100: score += 1
    if _len('working_hours') >= 30: score += 1
    if _len('holidays') >= 30: score += 1
    if _len('welcome_requirements') >= 10: score += 1
    if _len('facility_scale') >= 5: score += 1
    if _len('staff_composition') >= 10: score += 1
    if _len('established_date') >= 4: score += 1

    return min(score, 10)


# ============================================================
# メイン関数
# ============================================================

def analyze_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """生CSVのDataFrameに全属性カラムを追加して返す"""
    out = df.copy()

    # カラムマッピング（元カラムを保持しつつ、classifier期待名で複写）
    mapping = {
        "募集職種": "headline",
        "仕事内容": "job_description",
        "応募要件": "requirements",
        "歓迎要件": "welcome_requirements",
        "待遇": "benefits",
        "給与の備考": "salary_detail",
        "勤務時間": "working_hours",
        "休日": "holidays",
        "教育体制・研修": "education_training",
        "法人・施設名": "facility_name",
        "アクセス": "access",
        "施設・サービス形態": "service_type",
        "スタッフ構成": "staff_composition",
        "設立年月日": "established_date",
        "施設規模": "facility_scale",
        "長期休暇・特別休暇": "special_holidays",
        "給与_下限": "salary_min",
        "給与_上限": "salary_max",
        "給与_区分": "salary_type",
        "bottom_text": "selection_process",
    }
    for src, dst in mapping.items():
        if src in out.columns:
            out[dst] = out[src]

    # 雇用形態
    if "給与_雇用形態" in out.columns:
        out["employment_type"] = out["給与_雇用形態"].apply(clean_employment_type)
    else:
        out["employment_type"] = ""

    # フォールバック: employment_typeが空/NaNの場合、給与テキストから復元
    mask_empty = (
        out["employment_type"].isna()
        | (out["employment_type"] == '')
        | (out["employment_type"] == 'nan')
    )
    salary_col = "給与" if "給与" in out.columns else None
    if salary_col and mask_empty.any():
        recovered = out.loc[mask_empty, salary_col].apply(
            extract_employment_from_salary_text
        )
        out.loc[mask_empty, "employment_type"] = recovered
        recovered_count = (recovered != '').sum()
        if recovered_count > 0:
            print(f"    employment_type復元: {recovered_count}/{mask_empty.sum()}件を給与テキストから復元")

    # 都道府県・市区町村
    if "アクセス" in out.columns:
        parsed = out["アクセス"].apply(parse_access)
        out["prefecture"] = parsed.apply(lambda x: x[0])
        out["municipality"] = parsed.apply(lambda x: x[1])

    # 構造化属性
    out["required_experience_years"] = out["requirements"].apply(
        lambda x: extract_required_experience_years(x) if isinstance(x, str) else 0
    )
    out["age_limit"] = out["requirements"].apply(
        lambda x: extract_age_limit(x) if isinstance(x, str) else np.nan
    )
    out["annual_holidays"] = out["holidays"].apply(
        lambda x: extract_annual_holidays_num(x) if isinstance(x, str) else np.nan
    )
    out["bonus_count"] = out.apply(
        lambda r: extract_bonus_count(
            r.get("benefits", ""), r.get("salary_detail", "")
        ), axis=1
    )

    # タグ
    out["tags"] = out.apply(extract_tags, axis=1)

    # Benefitsフラグ
    flags_df = out.apply(extract_benefits_flags, axis=1, result_type='expand')
    out = pd.concat([out, flags_df], axis=1)
    out["benefits_score"] = flags_df.sum(axis=1)

    # コンテンツ充実度
    out["content_richness_score"] = out.apply(compute_content_richness_score, axis=1)

    # photo_count (CSVに情報なし)
    out["photo_count"] = 0

    # 仕事内容分析 (v1.2)
    jd_df = out.apply(analyze_job_description, axis=1, result_type='expand')
    out = pd.concat([out, jd_df], axis=1)

    # 年代セグメント検出 (v2.0)
    age_df = out.apply(detect_age_decade, axis=1, result_type='expand')
    out = pd.concat([out, age_df], axis=1)

    # 性別・女性ライフステージ検出 (v2.0)
    gl_df = out.apply(detect_gender_lifecycle, axis=1, result_type='expand')
    out = pd.concat([out, gl_df], axis=1)

    # 未経験×資格 細分化 (v2.0)
    eq_df = out.apply(detect_experience_qualification, axis=1, result_type='expand')
    out = pd.concat([out, eq_df], axis=1)

    # 勤務時間帯分析 (v2.1)
    if 'working_hours' in out.columns:
        wh_results = out['working_hours'].apply(parse_working_hours)
        wh_df = pd.DataFrame(wh_results.tolist(), index=out.index)
        out = pd.concat([out, wh_df], axis=1)

    # 休日パターン抽出 (v2.2)
    if 'holidays' in out.columns:
        hol_results = out['holidays'].apply(parse_holidays)
        hol_df = pd.DataFrame(hol_results.tolist(), index=out.index)
        out = pd.concat([out, hol_df], axis=1)

    return out
