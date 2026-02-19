/**
 * app.js - ECharts初期化 + HTMXイベント連携
 *
 * data-chart-config属性を持つ.echart要素を自動検出し、
 * EChartsインスタンスを初期化する。HTMXタブ切替後も再描画。
 */
(function() {
    'use strict';

    // ECharts初期化: 対象コンテナ内の.echart[data-chart-config]を走査
    function initECharts(container) {
        if (!container) container = document;
        var elements = container.querySelectorAll('.echart[data-chart-config]');
        elements.forEach(function(el) {
            // 高さ0の要素はスキップ（非表示タブ等）
            if (el.offsetHeight === 0) return;

            // 既存インスタンスをdispose（メモリリーク防止）
            var existing = echarts.getInstanceByDom(el);
            if (existing) {
                existing.dispose();
            }

            try {
                var configStr = el.getAttribute('data-chart-config');
                var config = JSON.parse(configStr);
                // JSON文字列中の"function(...){...}"をJavaScript関数に変換
                reviveFunctions(config);
                var chart = echarts.init(el, 'dark');
                // 背景色をnavy-900に合わせる
                config.backgroundColor = config.backgroundColor || 'transparent';
                chart.setOption(config);
                // コンテナリサイズ時にチャートも追従
                new ResizeObserver(function() { chart.resize(); }).observe(el);
            } catch (e) {
                console.warn('[app.js] ECharts初期化エラー:', e.message, el);
            }
        });
    }

    // JSON内の"function(...){...}"文字列を実際のJS関数に変換（再帰）
    // 注意: data-chart-configはサーバー側Rustコードから生成された信頼済みデータのみ。
    // ユーザー入力を含まないため、Function()コンストラクタの使用は安全。
    function reviveFunctions(obj) {
        if (!obj || typeof obj !== 'object') return;
        var fnPattern = /^function\s*\(([^)]*)\)\s*\{([\s\S]*)\}$/;
        Object.keys(obj).forEach(function(key) {
            var val = obj[key];
            if (typeof val === 'string' && fnPattern.test(val)) {
                try {
                    var m = val.match(fnPattern);
                    obj[key] = new Function(m[1], m[2]);
                } catch(e) { /* 変換失敗時は文字列のまま */ }
            } else if (Array.isArray(val)) {
                val.forEach(function(item) { reviveFunctions(item); });
            } else if (typeof val === 'object' && val !== null) {
                reviveFunctions(val);
            }
        });
    }

    // Leaflet地図初期化（maps.jsが読み込まれている場合に委譲）
    function initMaps(container) {
        if (typeof window.initLeafletMaps === 'function') {
            window.initLeafletMaps(container);
        }
    }

    // HTMXコンテンツ挿入後のイベント（タブ切替時に発火）
    document.body.addEventListener('htmx:afterSettle', function(evt) {
        var target = evt.detail.target || document;
        initECharts(target);
        initMaps(target);
    });

    // DOMContentLoaded: 初回ロード時の初期化
    document.addEventListener('DOMContentLoaded', function() {
        initECharts(document);
        initMaps(document);
    });

    // ウィンドウリサイズ時: 全EChartsインスタンスをリサイズ
    var resizeTimer = null;
    window.addEventListener('resize', function() {
        if (resizeTimer) clearTimeout(resizeTimer);
        resizeTimer = setTimeout(function() {
            var charts = document.querySelectorAll('.echart[data-chart-config]');
            charts.forEach(function(el) {
                var instance = echarts.getInstanceByDom(el);
                if (instance) {
                    instance.resize();
                }
            });
            // Leaflet地図もリサイズ
            if (typeof window.resizeLeafletMaps === 'function') {
                window.resizeLeafletMaps();
            }
        }, 200);
    });
})();
