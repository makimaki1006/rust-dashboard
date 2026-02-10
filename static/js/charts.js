// ECharts初期化ヘルパー

// Okabe-Itoカラーパレット（色弱対応）
var COLOR_PALETTE = ['#0072B2', '#E69F00', '#CC79A7', '#009E73', '#F0E442', '#D55E00', '#56B4E9'];

// ダークテーマ共通設定
var DARK_THEME = {
    backgroundColor: 'transparent',
    textStyle: { color: '#e2e8f0' },
    legend: { textStyle: { color: '#94a3b8' } },
    tooltip: {
        backgroundColor: 'rgba(15, 23, 42, 0.95)',
        borderColor: 'rgba(148, 163, 184, 0.22)',
        textStyle: { color: '#f8fafc' }
    },
    xAxis: {
        axisLine: { lineStyle: { color: '#334155' } },
        axisLabel: { color: '#94a3b8' },
        splitLine: { lineStyle: { color: 'rgba(148, 163, 184, 0.08)' } }
    },
    yAxis: {
        axisLine: { lineStyle: { color: '#334155' } },
        axisLabel: { color: '#94a3b8' },
        splitLine: { lineStyle: { color: 'rgba(148, 163, 184, 0.08)' } }
    }
};

/**
 * コンテナ内の全 .echart 要素を初期化
 * data-chart-config 属性のJSONからオプションを読み込む
 */
function initECharts(container) {
    container.querySelectorAll('.echart').forEach(function(el) {
        var configStr = el.getAttribute('data-chart-config');
        if (!configStr) return;

        try {
            var config = JSON.parse(configStr);

            // 既存インスタンスがあれば破棄
            var existing = echarts.getInstanceByDom(el);
            if (existing) existing.dispose();

            var chart = echarts.init(el, null, { renderer: 'canvas' });

            // ダークテーマをマージ
            var option = mergeDeep({}, DARK_THEME, config);
            if (!option.color) option.color = COLOR_PALETTE;

            chart.setOption(option);
        } catch (e) {
            console.error('ECharts init error:', e, configStr);
        }
    });
}

/**
 * ディープマージユーティリティ
 */
function mergeDeep(target) {
    for (var i = 1; i < arguments.length; i++) {
        var source = arguments[i];
        if (!source) continue;
        for (var key in source) {
            if (source[key] && typeof source[key] === 'object' && !Array.isArray(source[key])) {
                target[key] = target[key] || {};
                mergeDeep(target[key], source[key]);
            } else {
                target[key] = source[key];
            }
        }
    }
    return target;
}
