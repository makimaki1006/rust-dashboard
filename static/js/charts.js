/**
 * charts.js - EChartsダークテーマカスタム + ヘルパー関数
 *
 * navy-900背景に最適化されたダークテーマを登録。
 * 数値フォーマットヘルパーを提供。
 */
(function() {
    'use strict';

    // ダークテーマのカスタマイズ（ECharts組み込みdarkテーマの上書き）
    // echarts.init(dom, 'dark')使用時に適用される
    if (typeof echarts !== 'undefined' && echarts.registerTheme) {
        echarts.registerTheme('dark', {
            backgroundColor: 'transparent',
            textStyle: {
                color: '#e2e8f0'
            },
            title: {
                textStyle: { color: '#f8fafc' },
                subtextStyle: { color: '#94a3b8' }
            },
            legend: {
                textStyle: { color: '#cbd5e1' }
            },
            tooltip: {
                backgroundColor: 'rgba(15, 23, 42, 0.95)',
                borderColor: '#334155',
                textStyle: { color: '#e2e8f0' }
            },
            categoryAxis: {
                axisLine: { lineStyle: { color: '#334155' } },
                axisTick: { lineStyle: { color: '#475569' } },
                axisLabel: { color: '#94a3b8' },
                splitLine: { lineStyle: { color: '#1e293b' } }
            },
            valueAxis: {
                axisLine: { lineStyle: { color: '#334155' } },
                axisTick: { lineStyle: { color: '#475569' } },
                axisLabel: { color: '#94a3b8' },
                splitLine: { lineStyle: { color: '#1e293b' } }
            },
            // カラーパレット（Wong配色ベース: 色覚バリアフリー対応）
            color: [
                '#0072B2', '#E69F00', '#009E73', '#D55E00',
                '#CC79A7', '#56B4E9', '#F0E442', '#999999'
            ]
        });
    }

    // 数値フォーマットヘルパー
    window.ChartHelpers = {
        // カンマ区切り
        formatNumber: function(num) {
            if (num == null || isNaN(num)) return '-';
            return Number(num).toLocaleString('ja-JP');
        },

        // 円表示（例: 280,000円）
        formatYen: function(num) {
            if (num == null || isNaN(num)) return '-';
            return Number(num).toLocaleString('ja-JP') + '円';
        },

        // 万円表示（例: 28.0万円）
        formatManYen: function(num) {
            if (num == null || isNaN(num)) return '-';
            return (Number(num) / 10000).toFixed(1) + '万円';
        },

        // パーセント表示（例: 45.2%）
        formatPercent: function(num, digits) {
            if (num == null || isNaN(num)) return '-';
            return Number(num).toFixed(digits != null ? digits : 1) + '%';
        },

        // EChartsインスタンスを安全に取得
        getInstance: function(domOrId) {
            var el = typeof domOrId === 'string' ? document.getElementById(domOrId) : domOrId;
            return el ? echarts.getInstanceByDom(el) : null;
        }
    };
})();
