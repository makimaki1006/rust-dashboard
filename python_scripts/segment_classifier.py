#!/usr/bin/env python3
"""
ジョブメドレー求人 階層型セグメント分類エンジン v2.3

v1.0: 初版 - 5軸×22中分類×20パターン
v1.1: 354,290件の実データフィードバックによるチューニング
  - B1偏重対策: 新卒可タグ 3→1、年齢上限30に厳格化
  - B5強化: 年齢不問タグ 2→3、単一年齢タグでも加点
  - C3強化: 主婦OK 2→3、育児支援/家庭都合休 1→2
  - D1抑制: 想定年収 2→1、明示的高収入のみ高加点
  - E2抑制: タグ条件15→20、コンテンツ閾値7→9、E3ベース2→3
  - Tier3辞書: 20→41パターンに拡張（カバレッジ46.5%→推定79%）
v2.3: スコアリング偏重是正 + タグ検出強化
  - E5厳格化: QUIET_POST 42.9%→目標15-20%（複合条件3項目以上要求）
  - E3動的化: 通常求人（content4-6,desc100-500字,タグ6-14）でE3=6点
  - C3上限制限: タグ重み減、テキスト上限4、全体上限8点
  - A1汎用タグ依存低減: 未経験可 2→1、全体上限8点
  - 属性活用拡大: C1(service_type), C2(overtime/shift), D2(3点揃い), E4(exp_qual)
  - Tier3: 47パターン（+6 AUTO頻出パターン名前付き化）

辞書ドキュメントに基づく3層分類:
  Tier1 (大分類): 5軸
  Tier2 (中分類): 22種
  Tier3 (小分類): 47パターン（20初期 + 21データドリブン + 6 v2.3追加）

依存: job_medley_analyzer.py の出力CSV（256属性抽出済み）
"""

import pandas as pd
import numpy as np
import re
import json
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')


# ============================================================
# 1. Tier2 中分類の定義 & スコアリングルール
# ============================================================

class Tier2Scorer:
    """各軸の中分類スコアを算出するクラス"""

    def __init__(self, row, all_text="", tags=""):
        self.r = row
        self.text = str(all_text)
        self.tags = str(tags)

    def _tag(self, keyword):
        return 1 if keyword in self.tags else 0

    def _txt(self, pattern):
        return 1 if re.search(pattern, self.text) else 0

    def _txt_count(self, patterns):
        return sum(1 for p in patterns if re.search(p, self.text))

    def _val(self, col, default=0):
        v = self.r.get(col)
        if pd.isna(v): return default
        return v

    # ──────────────────────────────────
    # 軸A: 経験レベル
    # ──────────────────────────────────
    def score_A1(self):
        """完全未経験歓迎"""
        req_years = self._val('required_experience_years', 0)
        if req_years > 0: return -99
        s = 0
        s += 1 * self._tag('未経験可')  # v2.3: 2→1（65%超の求人に存在する汎用タグ）
        s += 2 * self._tag('無資格可')
        s += self._tag('資格不問')
        s += self._tag('学歴不問')
        s += 2 * self._tag('新卒可')
        s += self._txt_count([r'一から', r'イチから', r'ゼロから', r'初めての方', r'はじめての方'])
        s += self._txt_count([r'丁寧に.{0,5}教え', r'安心してスタート', r'しっかり(サポート|フォロー)'])
        # v2.0: exp_qual_segment活用
        eqs = str(self._r_get('exp_qual_segment', ''))
        if eqs == '未経験＋無資格': s += 3
        elif eqs == '未経験＋有資格': s -= 2  # A2に流す
        # 必須資格がある場合は A2 に流す
        has_required_cert = self._txt_count([r'(介護福祉士|初任者研修|実務者研修|正看護師|保育士).{0,5}(必須|必要)'])
        if has_required_cert > 0: s -= 3
        return min(s, 8)  # v2.3: 全体上限8点

    def score_A2(self):
        """未経験可（資格あり）"""
        req_years = self._val('required_experience_years', 0)
        if req_years > 0: return -99
        s = 0
        s += 2 * self._tag('未経験可')
        has_cert = self._txt_count([
            r'(介護福祉士|初任者研修|実務者研修|正看護師|准看護師|保育士).{0,5}(必須|必要|以上)',
            r'(資格|免許).{0,5}(お持ち|をお持ち|ある方)'
        ])
        s += 2 * min(has_cert, 1)
        s += self._txt_count([r'資格さえ', r'資格があれば', r'訪問.{0,5}未経験.{0,3}(OK|可)'])
        if has_cert == 0: s -= 5
        return s

    def score_A3(self):
        """軽度経験（1-2年）"""
        req_years = self._val('required_experience_years', 0)
        if req_years == 0 or req_years > 2: return -99
        s = 3
        s += self._tag('ブランク可')
        s += self._txt_count([r'経験(が)?浅くても', r'少しでも経験'])
        return s

    def score_A4(self):
        """即戦力経験者"""
        req_years = self._val('required_experience_years', 0)
        s = 0
        if req_years >= 3: s += 4
        elif req_years >= 1: s += 1
        s += self._txt_count([r'即戦力', r'管理(職)?経験', r'リーダー経験', r'経験者(優遇|歓迎|求む)'])
        s += self._txt_count([r'臨床経験\d+年', r'実務経験\d+年以上'])
        if self._tag('未経験可'): s -= 2
        return s

    def score_A5(self):
        """復職・ブランク者"""
        s = 0
        s += 2 * self._tag('ブランク可')
        s += 2 * self._tag('復職支援')
        s += self._txt_count([r'ブランク.{0,5}(OK|可|歓迎|問いません|ある方も)',
                              r'復職', r'現場復帰', r'お仕事復帰', r'育児.{0,5}(明け|から)'])
        # v2.0: exp_qual_segment活用（経験者＋有資格でブランク→A5に誘導）
        eqs = str(self._r_get('exp_qual_segment', ''))
        if eqs == '経験者＋有資格' and self._tag('ブランク可'): s += 2
        # 新卒可がある場合はA5よりA1/A2を優先すべき
        if self._tag('新卒可'): s -= 2
        return s

    # ──────────────────────────────────
    # 軸B: 年齢・キャリアステージ
    # ──────────────────────────────────
    def score_B1(self):
        """新卒・第二新卒"""
        s = 0
        s += 1 * self._tag('新卒可')  # v1.1: 3→1に下げ（B1偏重対策）
        s += 2 * self._txt_count([r'新卒(歓迎|募集|採用)', r'第二新卒', r'2[5-8]卒'])  # 明示的な新卒訴求のみ高加点
        s += self._txt_count([r'新卒', r'卒業見込'])
        age_limit = self._val('age_limit')
        if pd.notna(age_limit) and age_limit <= 30: s += 2  # v1.1: 35→30に厳格化
        elif pd.notna(age_limit) and age_limit <= 35: s += 1
        return s

    def score_B2(self):
        """若手成長層（20-30代）"""
        s = 0
        s += self._txt_count([r'20代', r'30代', r'若手', r'キャリアアップ', r'キャリアパス',
                              r'ステップアップ', r'成長できる', r'将来.{0,5}(幹部|管理職|リーダー)'])
        # v1.2: テキストベースの年齢シグナル（20-30代向け）
        s += self._txt_count([r'髪(色|型).{0,3}(自由|OK)', r'ネイル.{0,3}(自由|OK)',
                              r'ピアス.{0,3}(自由|OK)', r'服装.{0,3}自由',
                              r'20.{0,2}30代.{0,5}(中心|活躍|多い)',
                              r'平均年齢.{0,3}(2\d|3[0-5])歳'])
        age_limit = self._val('age_limit')
        if pd.notna(age_limit) and 36 <= age_limit <= 45: s += 1
        # v2.0: age_decade_primary活用（テキスト解析結果）
        adp = str(self._r_get('age_decade_primary', ''))
        if adp in ('20代', '30代'): s += 2
        # 新卒タグなし（B1と分離）
        if not self._tag('新卒可'): s += 1
        # 研修・教育充実
        edu_len = len(str(self._r_get('education_training', '')))
        if edu_len > 200: s += 1
        return s

    def _r_get(self, col, default=''):
        v = self.r.get(col)
        if pd.isna(v): return default
        return v

    def score_B3(self):
        """ミドル層（30-50代）"""
        s = 0
        s += 2 * self._tag('40代活躍')
        s += self._txt_count([r'40代', r'ミドル', r'30代.{0,5}40代', r'管理職経験', r'即戦力'])
        # v1.2: テキストベースの年齢シグナル（30-50代向け）
        s += self._txt_count([r'家庭.{0,5}(両立|と両立)', r'子育て.{0,5}(しながら|中の方)',
                              r'(30|40)代.{0,5}(中心|活躍|多い)',
                              r'平均年齢.{0,3}(3[5-9]|4\d)歳',
                              r'経験(を|が)活かせる'])
        age_limit = self._val('age_limit')
        if pd.notna(age_limit) and 50 <= age_limit <= 64: s += 1
        # v2.0: age_decade_primary活用
        adp = str(self._r_get('age_decade_primary', ''))
        if adp in ('40代', '50代'): s += 2
        elif adp == '30代': s += 1  # 30代はB2/B3両方に加点
        return s

    def score_B4(self):
        """シニア層（50代〜）"""
        s = 0
        s += 2 * self._tag('50代活躍')
        s += 2 * self._tag('60代活躍')
        s += self._txt_count([r'50代', r'60代', r'シニア', r'セカンドキャリア', r'定年後'])
        age_limit = self._val('age_limit')
        if pd.notna(age_limit) and age_limit >= 65: s += 1
        # 再雇用制度
        s += self._txt_count([r'再雇用(制度)?あり', r'定年(後|65|70)'])
        return s

    def score_B5(self):
        """年齢不問・幅広い層"""
        s = 0
        s += 3 * self._tag('年齢不問')  # v1.1: 2→3に引き上げ
        # 複数年齢タグが同時にある場合
        age_tags = sum([self._tag('40代活躍'), self._tag('50代活躍'), self._tag('60代活躍')])
        if age_tags >= 2: s += 3  # v1.1: 2→3
        elif age_tags == 1: s += 1  # v1.1: 1タグでも加点
        s += self._txt_count([r'年齢.{0,3}(不問|問いません|問わず)', r'幅広い年齢',
                              r'年齢(は|を)問', r'どなたでも'])
        return s

    # ──────────────────────────────────
    # 軸C: ライフスタイル・働き方
    # ──────────────────────────────────
    def score_C1(self):
        """フルタイム・キャリア志向"""
        s = 0
        emp = str(self._r_get('employment_type', ''))
        if '正職員' in emp or '正社員' in emp: s += 2
        s += self._tag('資格取得支援')
        s += self._tag('研修制度あり')
        s += self._txt_count([r'キャリアアップ', r'キャリアパス', r'スキルアップ', r'昇格', r'昇進',
                              r'管理職', r'リーダー', r'ステップアップ'])
        edu_len = len(str(self._r_get('education_training', '')))
        if edu_len > 300: s += 1
        # v2.3: service_type活用（病院/大規模施設はキャリア志向の場）
        svc = str(self._r_get('service_type', ''))
        if re.search(r'病院|総合病院|大学病院|医療センター', svc): s += 1
        return s

    def score_C2(self):
        """ワークライフバランス重視"""
        s = 0
        s += 2 * self._tag('日勤のみ可')
        s += 2 * self._tag('残業ほぼなし')
        s += self._tag('年間休日120日以上')
        s += self._tag('4週8休以上')
        s += self._txt_count([r'ワークライフバランス', r'プライベート.{0,5}(充実|大切|両立)',
                              r'残業(ほぼ)?なし', r'定時退社', r'持ち帰り.{0,5}なし',
                              r'週休3日'])
        # v1.2: テレワーク/フレックス検出
        s += 2 * self._txt_count([r'テレワーク', r'リモートワーク', r'在宅勤務'])
        s += self._txt_count([r'フレックス(タイム)?', r'時差出勤'])
        annual_h = self._val('annual_holidays')
        if pd.notna(annual_h) and annual_h >= 120: s += 1
        # v2.3: 勤務時間帯属性活用
        overtime = str(self._r_get('wh_overtime', ''))
        if overtime in ('残業なし', '残業ほぼなし'): s += 1
        shift_type = str(self._r_get('wh_shift_type', ''))
        if shift_type in ('日勤のみ', '固定時間'): s += 1
        return s

    def score_C3(self):
        """子育て・家庭両立型"""
        s = 0
        # v2.3: タグ重みを抑制（C3スコア過大防止）
        s += 2 * self._tag('主夫・主婦OK')   # v2.3: 3→2
        s += 1 * self._tag('育児支援あり')   # v2.3: 2→1
        s += 1 * self._tag('家庭都合休OK')   # v2.3: 2→1
        s += min(self._txt_count([r'主婦', r'主夫', r'扶養内', r'扶養範囲', r'家庭と両立',
                              r'子育て(しながら|中|ママ)', r'育児中', r'時短勤務',
                              r'お子さん.{0,5}(いる|いらっしゃる)', r'託児',
                              r'産(前|後)休暇', r'育(児|休)', r'ママ(さん)?歓迎']), 4)  # v2.3: テキスト上限4
        # v2.0: lifecycle_primary活用
        lcp = str(self._r_get('lifecycle_primary', ''))
        if lcp in ('育児期', '結婚・出産期'): s += 2  # v2.3: 3→2
        elif lcp == '復職期': s += 1
        return min(s, 8)  # v2.3: 全体上限8点

    def score_C4(self):
        """Wワーク・副業・短時間"""
        s = 0
        s += 2 * self._tag('副業OK')
        s += 2 * self._tag('WワークOK')
        s += self._tag('フリーターOK')
        emp = str(self._r_get('employment_type', ''))
        if 'パート' in emp or 'バイト' in emp: s += 2
        s += self._txt_count([r'フリーター', r'Wワーク', r'ダブルワーク', r'副業', r'掛け持ち',
                              r'スキマ時間', r'短時間'])
        return s

    def score_C5(self):
        """安定・長期就業型"""
        s = 0
        s += self._tag('退職金あり')
        s += self._txt_count([r'退職金', r'永年勤続', r'勤続表彰', r'長く働', r'腰を据え',
                              r'正(社員|職員)登用', r'再雇用(制度)?あり', r'安定した(経営|基盤)',
                              r'創業\d+年', r'設立\d+年'])
        return s

    # ──────────────────────────────────
    # 軸D: 求職動機・訴求軸
    # ──────────────────────────────────
    def score_D1(self):
        """収入アップ訴求 (v2.4: 閾値引上げで偏重是正)"""
        s = 0
        s += 1 * self._txt_count([r'想定年収', r'モデル年収', r'年収\d{3}万'])
        s += 2 * self._txt_count([r'高(給|収入|年収|時給)', r'インセンティブ', r'歩合'])
        s += self._txt_count([r'お祝い金', r'入社祝', r'支度金'])
        bonus_months = self._val('bonus_count', 0)
        if bonus_months >= 4: s += 1
        # v2.4: 給与閾値引上げ（看護師月給中央値=260,000を考慮）
        salary_min = self._val('salary_min', 0)
        salary_max = self._val('salary_max', 0)
        if salary_min >= 350000: s += 2    # v2.4: 300K→350K（上位25%）
        elif salary_min >= 300000: s += 1  # v2.4: 250K→300K（上位50%）
        # 250,000は中央値以下のため加点なし
        if salary_max >= 450000: s += 1    # v2.4: 400K→450K
        return s

    def score_D2(self):
        """安定性・規模訴求"""
        s = 0
        s += self._txt_count([r'上場', r'大手', r'全国\d+.{0,3}(事業所|拠点|施設)',
                              r'創業\d+年', r'設立\d+年', r'安定(した|の)(経営|基盤|運営)',
                              r'退職金', r'永年勤続', r'持株会'])
        # v2.3: 退職金+賞与+社保完備の3点揃いは安定性の強シグナル
        stability_flags = (self._val('has_退職金', 0) + self._val('has_賞与', 0)
                           + self._val('has_社会保険完備', 0))
        if stability_flags >= 3: s += 2
        return s

    def score_D3(self):
        """理念・やりがい訴求 (v2.4: テキストマッチ+1底上げ)"""
        s = 0
        count = self._txt_count([r'理念', r'ビジョン', r'ミッション', r'社会貢献',
                              r'地域.{0,5}(貢献|密着|支え)', r'やりがい', r'想い', r'志',
                              r'未来', r'100年', r'その人らしく'])
        if count > 0:
            s += count + 1  # v2.4: 1個でも2点に（底上げ）
        return s

    def score_D4(self):
        """職場環境・人間関係訴求 (v2.4: テキストマッチ+1底上げ)"""
        s = 0
        count = self._txt_count([r'アットホーム', r'風通し.{0,5}良', r'チームワーク',
                              r'人間関係.{0,5}良', r'相談.{0,5}(できる|しやすい)',
                              r'先輩.{0,5}(サポート|フォロー)', r'仲間', r'雰囲気.{0,5}良',
                              r'話しやすい', r'意見.{0,5}(出せる|言える|反映)'])
        if count > 0:
            s += count + 1  # v2.4: 1個でも2点に（底上げ）
        return s

    def score_D5(self):
        """利便性・働きやすさ訴求"""
        s = 0
        s += self._txt_count([r'電動(自転車|アシスト)', r'スマ(ホ|ートフォン).{0,5}貸与',
                              r'直行直帰', r'ペーパーレス', r'ICT', r'IT.{0,5}(化|活用|導入)',
                              r'(自転車|バイク|車).{0,5}貸与', r'駅.{0,5}(近|チカ|徒歩\d分)',
                              r'電子カルテ', r'タブレット'])
        return s

    def score_D6(self):
        """成長・スキルアップ訴求 (v2.4: education閾値緩和)"""
        s = 0
        s += self._txt_count([r'研修.{0,5}(充実|豊富|10|多数)', r'e-?ラーニング',
                              r'キャリアパス.{0,5}(2|3|複数)', r'資格取得.{0,5}(支援|補助|制度)',
                              r'社内認定', r'スキルアップ', r'キャリアアップ',
                              r'成長.{0,5}(できる|環境|実感)'])
        edu_len = len(str(self._r_get('education_training', '')))
        if edu_len > 300: s += 2
        elif edu_len > 100: s += 1  # v2.4: 150→100に緩和
        return s

    def score_D7(self):
        """条件・待遇訴求"""
        s = 0
        benefits_score = self._val('benefits_score', 0)
        # v1.2: 閾値をフラグ数増加(19→25)に合わせ上方調整
        if benefits_score >= 15: s += 3
        elif benefits_score >= 11: s += 2
        elif benefits_score >= 7: s += 1
        s += self._txt_count([r'福利厚生.{0,5}充実', r'待遇.{0,5}充実', r'手当.{0,5}充実',
                              r'ベネフィットステーション', r'リロクラブ'])
        return s

    # ──────────────────────────────────
    # 軸E: 採用姿勢・緊急度
    # ──────────────────────────────────
    def score_E1(self):
        """緊急大量採用"""
        s = 0
        s += 2 * self._txt_count([r'お祝い金', r'入社祝'])
        s += self._tag('即日勤務OK')
        s += self._tag('未経験可')
        s += self._tag('学歴不問')
        s += self._tag('フリーターOK')
        s += self._tag('主夫・主婦OK')
        # 面接1回
        s += self._txt_count([r'面(接|談)\s*(1|１)\s*回'])
        s += self._tag('オープン3年以内')
        return s

    def score_E2(self):
        """積極採用（間口広め） (v2.4: 複数名募集・増員検出追加)"""
        s = 0
        open_tags = sum([self._tag('未経験可'), self._tag('ブランク可'),
                         self._tag('40代活躍'), self._tag('50代活躍')])
        if open_tags >= 3: s += 2
        elif open_tags >= 2: s += 1
        content_score = self._val('content_richness_score', 0)
        if content_score >= 9: s += 2
        elif content_score >= 6: s += 1
        tag_count = len(str(self.tags).split(','))
        if tag_count >= 20: s += 1
        desc_len = len(str(self._r_get('job_description', '')))
        if desc_len >= 500: s += 1
        s += self._txt_count([r'動画', r'インタビュー', r'スタッフの声', r'先輩社員'])
        # v2.4: 複数名募集・増員の検出
        s += self._txt_count([r'(複数|[2-9])\s*名?\s*(募集|採用|枠)', r'増員'])
        return s

    def score_E3(self):
        """通常採用 (v2.4: v2.3ベース維持 + 微拡張)"""
        s = 3
        content_score = self._val('content_richness_score', 0)
        if 4 <= content_score <= 6: s += 1
        desc_len = len(str(self._r_get('job_description', '')))
        if 100 <= desc_len < 500: s += 1
        tag_count = len(str(self.tags).split(','))
        if 6 <= tag_count <= 14: s += 1
        return s

    def score_E4(self):
        """厳選採用"""
        s = 0
        req_years = self._val('required_experience_years', 0)
        if req_years >= 3: s += 2
        s += self._txt_count([r'面(接|談)\s*(2|２|3|３)\s*回', r'適性検査', r'書類選考',
                              r'筆記試験'])
        if not self._tag('未経験可'): s += 1
        # v2.3: exp_qual_segment活用（経験者・資格必須は厳選度が高い）
        eqs = str(self._r_get('exp_qual_segment', ''))
        if eqs == '経験者・資格必須': s += 2
        return s

    def score_E5(self):
        """欠員補充・静かな募集 (v2.4: 閾値1段階厳格化)"""
        content_score = self._val('content_richness_score', 0)
        tag_count = len(str(self.tags).split(','))
        desc_len = len(str(self._r_get('job_description', '')))
        benefits_len = len(str(self._r_get('benefits', '')))
        appeal_patterns = [r'アットホーム', r'風通し', r'チームワーク', r'やりがい',
                           r'成長', r'充実', r'安心', r'地域密着', r'理念']
        appeal_count = self._txt_count(appeal_patterns)

        low_quality_flags = 0
        if content_score <= 2: low_quality_flags += 1
        if tag_count <= 3: low_quality_flags += 1
        if desc_len < 50: low_quality_flags += 1
        if appeal_count == 0: low_quality_flags += 1
        if benefits_len < 20: low_quality_flags += 1

        # v2.3維持（4→4点, 3→2点, 2→1点）+ v2.4微調整不要
        if low_quality_flags >= 4: return 4
        if low_quality_flags >= 3: return 2
        if low_quality_flags >= 2: return 1
        return 0

    # ──────────────────────────────────
    # 全軸スコアリング
    # ──────────────────────────────────
    def score_all(self):
        results = {}

        # 軸A
        a_scores = {
            'A1': self.score_A1(), 'A2': self.score_A2(), 'A3': self.score_A3(),
            'A4': self.score_A4(), 'A5': self.score_A5(),
        }
        results['axis_A'] = a_scores
        results['tier2_A'] = max(a_scores, key=a_scores.get)
        results['tier2_A_score'] = max(a_scores.values())

        # 軸B
        b_scores = {
            'B1': self.score_B1(), 'B2': self.score_B2(), 'B3': self.score_B3(),
            'B4': self.score_B4(), 'B5': self.score_B5(),
        }
        results['axis_B'] = b_scores
        results['tier2_B'] = max(b_scores, key=b_scores.get)
        results['tier2_B_score'] = max(b_scores.values())

        # 軸C
        c_scores = {
            'C1': self.score_C1(), 'C2': self.score_C2(), 'C3': self.score_C3(),
            'C4': self.score_C4(), 'C5': self.score_C5(),
        }
        results['axis_C'] = c_scores
        results['tier2_C'] = max(c_scores, key=c_scores.get)
        results['tier2_C_score'] = max(c_scores.values())

        # 軸D
        d_scores = {
            'D1': self.score_D1(), 'D2': self.score_D2(), 'D3': self.score_D3(),
            'D4': self.score_D4(), 'D5': self.score_D5(), 'D6': self.score_D6(),
            'D7': self.score_D7(),
        }
        results['axis_D'] = d_scores
        results['tier2_D'] = max(d_scores, key=d_scores.get)
        results['tier2_D_score'] = max(d_scores.values())

        # 軸E
        e_scores = {
            'E1': self.score_E1(), 'E2': self.score_E2(), 'E3': self.score_E3(),
            'E4': self.score_E4(), 'E5': self.score_E5(),
        }
        results['axis_E'] = e_scores
        results['tier2_E'] = max(e_scores, key=e_scores.get)
        results['tier2_E_score'] = max(e_scores.values())

        return results


# ============================================================
# 2. Tier2 ラベル辞書
# ============================================================

TIER2_LABELS = {
    'A1': '完全未経験歓迎', 'A2': '未経験可（資格あり）', 'A3': '軽度経験（1-2年）',
    'A4': '即戦力経験者', 'A5': '復職・ブランク者',
    'B1': '新卒・第二新卒', 'B2': '若手成長層', 'B3': 'ミドル層',
    'B4': 'シニア層', 'B5': '年齢不問・幅広い層',
    'C1': 'フルタイム・キャリア志向', 'C2': 'WLB重視', 'C3': '子育て・家庭両立型',
    'C4': 'Wワーク・副業・短時間', 'C5': '安定・長期就業型',
    'D1': '収入アップ訴求', 'D2': '安定性・規模訴求', 'D3': '理念・やりがい訴求',
    'D4': '職場環境訴求', 'D5': '利便性訴求', 'D6': '成長・スキルアップ訴求', 'D7': '条件・待遇訴求',
    'E1': '緊急大量採用', 'E2': '積極採用', 'E3': '通常採用', 'E4': '厳選採用', 'E5': '静かな募集',
}


# ============================================================
# 3. Tier3 パターン辞書
# ============================================================

TIER3_PATTERNS = [
    {
        'id': 'FRESH_CAREER',
        'conditions': {'A': ['A1', 'A2'], 'B': ['B1', 'B2'], 'C': ['C1'], 'D': ['D6']},
        'label': '新卒・未経験から成長できるキャリア型ポジション',
        'label_short': '未経験×若手×キャリア成長',
        'label_proposal': '成長環境を重視する若手の未経験者向け求人',
    },
    {
        'id': 'FRESH_EASY',
        'conditions': {'A': ['A1'], 'B': ['B1', 'B2'], 'E': ['E1']},
        'label': '若手の未経験を大量に採用したいポジション',
        'label_short': '未経験×若手×緊急大量採用',
        'label_proposal': '未経験の若手を大量採用中の求人',
    },
    {
        'id': 'YOUNG_SKILLED',
        'conditions': {'A': ['A2'], 'B': ['B2'], 'D': ['D6']},
        'label': '資格を活かしてステップアップしたい若手向け',
        'label_short': '資格有×若手×成長',
        'label_proposal': '資格保有の若手がスキルアップを狙える求人',
    },
    {
        'id': 'YOUNG_INCOME',
        'conditions': {'A': ['A4'], 'B': ['B2'], 'D': ['D1']},
        'label': '経験ある若手が年収アップを狙えるポジション',
        'label_short': '即戦力×若手×高収入',
        'label_proposal': '即戦力の若手向け高収入求人',
    },
    {
        'id': 'MAMA_WLB',
        'conditions': {'A': ['A5', 'A1', 'A2'], 'C': ['C3'], 'D': ['D4']},
        'label': '子育て中の方が安心して復帰できる職場',
        'label_short': '復職・未経験×子育て×環境重視',
        'label_proposal': '子育て中の復職者向けの温かい職場環境の求人',
    },
    {
        'id': 'MAMA_PART',
        'conditions': {'C': ['C4', 'C3'], 'D': ['D4']},
        'label': '子育てママのスキマ時間パート',
        'label_short': '短時間×子育て×環境重視',
        'label_proposal': '子育て中の方向け短時間パート',
    },
    {
        'id': 'SENIOR_STABLE',
        'conditions': {'A': ['A3', 'A4'], 'B': ['B4'], 'C': ['C5']},
        'label': 'シニアが経験を活かして長く働ける安定職場',
        'label_short': '経験者×シニア×安定長期',
        'label_proposal': 'シニア経験者が安定して長期就業できる求人',
    },
    {
        'id': 'SENIOR_EASY',
        'conditions': {'A': ['A1', 'A2'], 'B': ['B4'], 'E': ['E1', 'E2']},
        'label': 'シニア未経験OK！人手不足で積極募集',
        'label_short': '未経験×シニア×積極採用',
        'label_proposal': 'シニアの未経験者も歓迎する積極採用求人',
    },
    {
        'id': 'MID_EXPERT',
        'conditions': {'A': ['A4'], 'B': ['B3'], 'D': ['D2']},
        'label': 'ミドル世代の即戦力を安定企業が求める',
        'label_short': '即戦力×ミドル×安定企業',
        'label_proposal': 'ミドル世代の即戦力向け安定企業の求人',
    },
    {
        'id': 'MID_LEADER',
        'conditions': {'A': ['A4'], 'B': ['B3'], 'D': ['D6']},
        'extra_text': [r'管理職', r'リーダー', r'マネージャー', r'主任', r'施設長'],
        'label': '管理職・リーダー候補のミドル採用',
        'label_short': '即戦力×ミドル×管理職候補',
        'label_proposal': 'ミドル世代の管理職候補を求める求人',
    },
    {
        'id': 'RETURN_SAFE',
        'conditions': {'A': ['A5'], 'C': ['C2'], 'D': ['D4']},
        'label': 'ブランク明けでも安心のWLB重視職場',
        'label_short': '復職×WLB×環境重視',
        'label_proposal': '復職者がWLB重視で安心して働ける求人',
    },
    {
        'id': 'RETURN_CAREER',
        'conditions': {'A': ['A5'], 'C': ['C1'], 'D': ['D3']},
        'label': 'やりがいを持って現場復帰したい経験者向け',
        'label_short': '復職×キャリア×理念共感',
        'label_proposal': '理念に共感する復職経験者向けの求人',
    },
    {
        'id': 'DUAL_WORK',
        'conditions': {'C': ['C4'], 'D': ['D1']},
        'label': '副業・Wワークでしっかり稼ぎたい方向け',
        'label_short': 'Wワーク×収入重視',
        'label_proposal': 'Wワーク・副業で収入を増やしたい方向けの求人',
    },
    {
        'id': 'FREETER_START',
        'conditions': {'A': ['A1'], 'C': ['C4'], 'E': ['E1', 'E2']},
        'label': 'フリーターから正社員を目指せる入口',
        'label_short': '未経験×フリーター×積極採用',
        'label_proposal': 'フリーターの正社員化を支援する求人',
    },
    {
        'id': 'VISION_YOUNG',
        'conditions': {'A': ['A1', 'A2'], 'B': ['B2'], 'D': ['D3']},
        'label': '理念に共感する若手を育てたい事業所',
        'label_short': '未経験×若手×理念共感',
        'label_proposal': '理念重視で若手を育成する事業所の求人',
    },
    {
        'id': 'VISION_EXPERT',
        'conditions': {'A': ['A4'], 'D': ['D3', 'D6']},
        'label': '経験者がやりがいを持って長期成長できる環境',
        'label_short': '即戦力×理念×成長',
        'label_proposal': '経験者が理念を持って成長し続けられる求人',
    },
    {
        'id': 'MASS_HIRE',
        'conditions': {'E': ['E1'], 'A': ['A1']},
        'extra_text': [r'お祝い金.{0,10}\d{2}万'],
        'label': '大規模オープニング緊急大量採用',
        'label_short': '未経験×緊急大量×お祝い金',
        'label_proposal': 'お祝い金付きの大量緊急採用求人',
    },
    {
        'id': 'QUIET_POST',
        'conditions': {'E': ['E5']},
        'label': '最低限の掲載で静かに欠員補充',
        'label_short': '簡素掲載×欠員補充',
        'label_proposal': '控えめな掲載の欠員補充型求人',
    },
    {
        'id': 'PREMIUM_PKG',
        'conditions': {'D': ['D7']},
        'label': '業界トップクラスの待遇パッケージ',
        'label_short': '超充実待遇',
        'label_proposal': '福利厚生・待遇が非常に充実した求人',
    },
    {
        'id': 'IT_MODERN',
        'conditions': {'D': ['D5']},
        'label': 'IT活用で効率的に働ける現代型職場',
        'label_short': 'IT・利便性重視',
        'label_proposal': 'ICT活用で効率的な働き方ができる求人',
    },
    # ═══════════════════════════════════════════════════
    # v1.1: データドリブン追加パターン (354K件の頻出AUTOから)
    # ═══════════════════════════════════════════════════
    {
        'id': 'WLB_FRESH',
        'conditions': {'A': ['A1'], 'B': ['B1', 'B5'], 'C': ['C2'], 'D': ['D1', 'D2', 'D6']},
        'label': '未経験から始めるWLB重視の職場',
        'label_short': '未経験×WLB重視',
        'label_proposal': '未経験歓迎でワークライフバランスを重視する求人',
    },
    {
        'id': 'CAREER_FRESH',
        'conditions': {'A': ['A1'], 'B': ['B1', 'B5'], 'C': ['C1'], 'D': ['D1', 'D2']},
        'label': '未経験から正社員キャリアを築ける職場',
        'label_short': '未経験×正社員キャリア',
        'label_proposal': '未経験から正社員としてキャリアを築ける求人',
    },
    {
        'id': 'MAMA_FRESH',
        'conditions': {'A': ['A1', 'A2'], 'C': ['C3'], 'D': ['D1', 'D2', 'D6']},
        'label': '未経験ママが子育てしながら始められる職場',
        'label_short': '未経験×子育て両立',
        'label_proposal': '未経験で子育て中の方が始めやすい求人',
    },
    {
        'id': 'WLB_ANYONE',
        'conditions': {'A': ['A1', 'A5'], 'B': ['B5'], 'C': ['C2']},
        'label': '年齢不問でWLB重視の働きやすい職場',
        'label_short': '年齢不問×WLB重視',
        'label_proposal': '年齢を問わずWLBを重視できる求人',
    },
    {
        'id': 'RETURN_CAREER_YOUNG',
        'conditions': {'A': ['A5'], 'B': ['B1', 'B2'], 'C': ['C1']},
        'label': 'ブランクから若手としてキャリア再構築できる職場',
        'label_short': '復職×若手×キャリア再構築',
        'label_proposal': '若手の復職者がキャリアを再構築できる求人',
    },
    {
        'id': 'RETURN_WLB_WIDE',
        'conditions': {'A': ['A5'], 'B': ['B5'], 'C': ['C2']},
        'label': 'ブランク復帰で年齢不問×ゆったり勤務の職場',
        'label_short': '復職×年齢不問×WLB',
        'label_proposal': 'ブランク明けの方が年齢を問わずWLB重視で働ける求人',
    },
    {
        'id': 'RETURN_MID_CAREER',
        'conditions': {'A': ['A5'], 'B': ['B3'], 'C': ['C1']},
        'label': 'ミドルのブランク明けがキャリア復帰できる職場',
        'label_short': '復職×ミドル×キャリア復帰',
        'label_proposal': 'ミドル世代の復職者がキャリア復帰できる求人',
    },
    {
        'id': 'RETURN_CAREER_WIDE',
        'conditions': {'A': ['A5'], 'B': ['B5'], 'C': ['C1']},
        'label': 'ブランクから幅広い年齢でキャリア復帰',
        'label_short': '復職×年齢不問×キャリア',
        'label_proposal': 'ブランク明けの方が年齢不問でキャリア復帰できる求人',
    },
    {
        'id': 'RETURN_WLB_YOUNG',
        'conditions': {'A': ['A5'], 'B': ['B1', 'B2'], 'C': ['C2']},
        'label': '若手復帰者がWLBを重視して再スタートできる職場',
        'label_short': '復職×若手×WLB再スタート',
        'label_proposal': '若手の復職者がWLB重視で再スタートできる求人',
    },
    {
        'id': 'RETURN_MID_WLB',
        'conditions': {'A': ['A5'], 'B': ['B3'], 'C': ['C2']},
        'label': 'ミドル復帰者がゆったりペースで復帰できる職場',
        'label_short': '復職×ミドル×WLB',
        'label_proposal': 'ミドル世代の復職者がWLB重視で働ける求人',
    },
    {
        'id': 'SENIOR_RETURN_CAREER',
        'conditions': {'A': ['A5'], 'B': ['B4'], 'C': ['C1']},
        'label': 'シニア復帰者が経験を活かしてキャリア継続',
        'label_short': '復職×シニア×キャリア',
        'label_proposal': 'シニア世代の復職者がキャリアを活かせる求人',
    },
    {
        'id': 'CAREER_ANYONE',
        'conditions': {'A': ['A1'], 'B': ['B5'], 'C': ['C1']},
        'label': '年齢不問でキャリア志向の未経験歓迎職場',
        'label_short': '未経験×年齢不問×キャリア',
        'label_proposal': '年齢不問で未経験からキャリアを築ける求人',
    },
    {
        'id': 'SENIOR_RETURN_WLB',
        'conditions': {'A': ['A5'], 'B': ['B4'], 'C': ['C2']},
        'label': 'シニア復帰者がゆったりペースで働ける職場',
        'label_short': '復職×シニア×WLB',
        'label_proposal': 'シニア世代の復職者がWLB重視で働ける求人',
    },
    {
        'id': 'CERT_CAREER',
        'conditions': {'A': ['A2'], 'B': ['B1', 'B2', 'B5'], 'C': ['C1']},
        'label': '資格を活かして未経験からキャリア開始',
        'label_short': '資格有×未経験×キャリア開始',
        'label_proposal': '資格保有者が未経験からキャリアを始められる求人',
    },
    {
        'id': 'FRESH_STABLE',
        'conditions': {'A': ['A1'], 'B': ['B1', 'B5'], 'C': ['C5']},
        'label': '未経験から安定企業で長期就業できる職場',
        'label_short': '未経験×安定長期',
        'label_proposal': '未経験歓迎の安定企業で長期就業できる求人',
    },
    {
        'id': 'MAMA_MID_RETURN',
        'conditions': {'A': ['A5'], 'B': ['B3'], 'C': ['C3']},
        'label': '子育て中ミドルのブランク復帰を支援する職場',
        'label_short': '復職×ミドル×子育て両立',
        'label_proposal': '子育て中のミドル世代が復帰しやすい求人',
    },
    {
        'id': 'MID_FRESH_CAREER',
        'conditions': {'A': ['A1'], 'B': ['B3'], 'C': ['C1']},
        'label': 'ミドル未経験からのキャリア挑戦を歓迎する職場',
        'label_short': '未経験×ミドル×キャリア挑戦',
        'label_proposal': 'ミドル世代の未経験者がキャリアを始められる求人',
    },
    {
        'id': 'MID_FRESH_WLB',
        'conditions': {'A': ['A1'], 'B': ['B3'], 'C': ['C2']},
        'label': 'ミドル未経験がWLB重視で新しい分野に挑戦',
        'label_short': '未経験×ミドル×WLB',
        'label_proposal': 'ミドル世代の未経験者がWLB重視で働ける求人',
    },
    {
        'id': 'SENIOR_RETURN_PART',
        'conditions': {'A': ['A5'], 'B': ['B4'], 'C': ['C4']},
        'label': 'シニア復帰者の短時間・Wワーク',
        'label_short': '復職×シニア×短時間',
        'label_proposal': 'シニア世代の復職者が短時間で働ける求人',
    },
    {
        'id': 'RETURN_MAMA_WIDE',
        'conditions': {'A': ['A5'], 'B': ['B5'], 'C': ['C3']},
        'label': '年齢不問で復帰する子育て中の方向け',
        'label_short': '復職×年齢不問×子育て',
        'label_proposal': '子育て中の復職者が年齢不問で働ける求人',
    },
    {
        'id': 'SENIOR_MAMA_RETURN',
        'conditions': {'A': ['A5'], 'B': ['B4'], 'C': ['C3']},
        'label': 'シニアで子育て中の方の復帰を支援する職場',
        'label_short': '復職×シニア×子育て',
        'label_proposal': '子育て中のシニア世代が復帰しやすい求人',
    },
    # ═══════════════════════════════════════════════════
    # v2.3: 頻出AUTOパターンの名前付き化（AUTO比率8.8%→4-5%目標）
    # ═══════════════════════════════════════════════════
    {
        'id': 'FRESH_WLB_CAREER',
        'conditions': {'A': ['A1'], 'B': ['B2'], 'C': ['C2']},
        'label': '未経験の若手がWLB重視で成長できる職場',
        'label_short': '未経験×若手×WLB',
        'label_proposal': '未経験の若手がWLBを重視しながら成長できる求人',
    },
    {
        'id': 'MID_CAREER_WLB',
        'conditions': {'A': ['A4'], 'B': ['B3'], 'C': ['C2']},
        'label': '即戦力ミドルがWLB重視で活躍できる職場',
        'label_short': '即戦力×ミドル×WLB',
        'label_proposal': '即戦力のミドル世代がWLB重視で活躍できる求人',
    },
    {
        'id': 'ANYONE_STABLE_CAREER',
        'conditions': {'A': ['A1', 'A2'], 'B': ['B5'], 'C': ['C5']},
        'label': '年齢不問で未経験から安定長期就業できる職場',
        'label_short': '未経験×年齢不問×安定長期',
        'label_proposal': '年齢不問で未経験者が安定して長く働ける求人',
    },
    {
        'id': 'EXPERT_INCOME_WLB',
        'conditions': {'A': ['A4'], 'D': ['D1'], 'C': ['C2']},
        'label': '即戦力が高収入とWLBを両立できるポジション',
        'label_short': '即戦力×高収入×WLB',
        'label_proposal': '即戦力の経験者が高収入でWLBも実現できる求人',
    },
    {
        'id': 'MID_STABLE_ENV',
        'conditions': {'A': ['A4'], 'B': ['B3'], 'D': ['D4']},
        'label': '即戦力ミドルが職場環境を重視して活躍できる職場',
        'label_short': '即戦力×ミドル×環境重視',
        'label_proposal': '即戦力のミドル世代向けの環境重視型求人',
    },
    {
        'id': 'FRESH_MAMA_WLB',
        'conditions': {'A': ['A1', 'A2'], 'C': ['C3'], 'D': ['D4', 'D7']},
        'label': '未経験ママがWLB重視で子育てしながら働ける職場',
        'label_short': '未経験×子育て×環境・待遇重視',
        'label_proposal': '未経験の子育て中の方が環境・待遇重視で働ける求人',
    },
]


# ============================================================
# 4. Tier3 マッチングエンジン
# ============================================================

def match_tier3(tier2_results, all_text=""):
    """Tier2の結果からTier3パターンをマッチング"""
    t2 = {
        'A': tier2_results['tier2_A'],
        'B': tier2_results['tier2_B'],
        'C': tier2_results['tier2_C'],
        'D': tier2_results['tier2_D'],
        'E': tier2_results['tier2_E'],
    }

    best_match = None
    best_score = 0

    for pattern in TIER3_PATTERNS:
        conds = pattern['conditions']
        match_count = 0
        total_conds = len(conds)

        for axis, valid_values in conds.items():
            if t2.get(axis) in valid_values:
                match_count += 1

        # extra_text条件
        if 'extra_text' in pattern:
            extra_match = any(re.search(p, all_text) for p in pattern['extra_text'])
            if extra_match:
                match_count += 0.5
            total_conds += 0.5

        score = match_count / total_conds if total_conds > 0 else 0

        if score > best_score:
            best_score = score
            best_match = pattern

    # 完全一致 or 80%以上一致でマッチ
    if best_match and best_score >= 0.8:
        return {
            'tier3_id': best_match['id'],
            'tier3_label': best_match['label'],
            'tier3_label_short': best_match['label_short'],
            'tier3_label_proposal': best_match['label_proposal'],
            'tier3_match_score': round(best_score, 2),
        }

    # フォールバック: テンプレート生成
    return generate_fallback_label(t2)


def generate_fallback_label(t2):
    """辞書にマッチしない場合のテンプレート生成"""
    a_label = TIER2_LABELS.get(t2['A'], '')
    b_label = TIER2_LABELS.get(t2['B'], '')
    c_label = TIER2_LABELS.get(t2['C'], '')
    d_label = TIER2_LABELS.get(t2['D'], '')
    e_label = TIER2_LABELS.get(t2['E'], '')

    short = f"{a_label} \u00d7 {b_label} \u00d7 {c_label}"
    full = f"{b_label}で{a_label}の方が、{c_label}な働き方で{d_label}を実現できるポジション"
    proposal = f"{d_label}を重視する{b_label}の{a_label}向け求人（採用姿勢:{e_label}）"

    return {
        'tier3_id': f"AUTO_{t2['A']}_{t2['B']}_{t2['C']}",
        'tier3_label': full,
        'tier3_label_short': short,
        'tier3_label_proposal': proposal,
        'tier3_match_score': 0.0,
    }


# ============================================================
# 5. メイン分類パイプライン
# ============================================================

def classify_row(row):
    """1求人を3層分類する"""
    # テキスト結合
    text_fields = ['headline', 'job_description', 'requirements', 'benefits',
                   'salary_detail', 'working_hours', 'holidays', 'education_training',
                   'selection_process', 'staff_composition', 'special_holidays',
                   'welcome_requirements']
    all_text = ' '.join(str(row.get(f, '')) for f in text_fields if pd.notna(row.get(f)))
    tags = str(row.get('tags', ''))

    # Tier2 スコアリング
    scorer = Tier2Scorer(row, all_text, tags)
    t2_results = scorer.score_all()

    # Tier3 マッチング
    t3_results = match_tier3(t2_results, all_text)

    # 結果統合
    output = {}

    # Tier1 (大分類コード)
    output['tier1_experience'] = t2_results['tier2_A']
    output['tier1_career_stage'] = t2_results['tier2_B']
    output['tier1_lifestyle'] = t2_results['tier2_C']
    output['tier1_appeal'] = t2_results['tier2_D']
    output['tier1_urgency'] = t2_results['tier2_E']

    # Tier2 (中分類ラベル)
    output['tier2_experience'] = TIER2_LABELS[t2_results['tier2_A']]
    output['tier2_career_stage'] = TIER2_LABELS[t2_results['tier2_B']]
    output['tier2_lifestyle'] = TIER2_LABELS[t2_results['tier2_C']]
    output['tier2_appeal'] = TIER2_LABELS[t2_results['tier2_D']]
    output['tier2_urgency'] = TIER2_LABELS[t2_results['tier2_E']]
    output['tier2_combined'] = f"{t2_results['tier2_A']}+{t2_results['tier2_B']}+{t2_results['tier2_C']}+{t2_results['tier2_D']}+{t2_results['tier2_E']}"

    # Tier3 (小分類)
    output.update(t3_results)

    # デバッグ: 各軸のスコア
    for axis in ['A', 'B', 'C', 'D', 'E']:
        scores = t2_results[f'axis_{axis}']
        output[f'debug_scores_{axis}'] = json.dumps(scores, ensure_ascii=False)

    return output


def classify_dataframe(df):
    """DataFrame全体を分類"""
    print(f"分類処理開始: {len(df):,}件")
    results = df.apply(classify_row, axis=1, result_type='expand')
    df = pd.concat([df, results], axis=1)
    print("分類完了")
    return df
