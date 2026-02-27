/**
 * tabcache.js - スケルトンローディング + クライアントサイドタブキャッシュ
 *
 * Phase 1: タブ切替時にスケルトンHTMLを即座表示
 * Phase 2: タブHTMLをメモリキャッシュし、2回目以降はサーバーリクエストなしで表示
 *
 * セキュリティ:
 * - スケルトンは静的定数（ユーザー入力不含）
 * - キャッシュはサーバーからの信頼済みHTMXレスポンスのみ保存・復元
 * - innerHTML使用箇所はすべてサーバー生成の信頼済みHTMLのみ
 */
(function() {
    'use strict';

    // ===== Phase 1: スケルトンローディング =====

    // スケルトンカードを安全にDOM生成する関数
    function createSkeletonCard(height, width) {
        var div = document.createElement('div');
        div.className = 'skeleton skeleton-card';
        div.style.height = height;
        if (width) div.style.width = width;
        return div;
    }

    function createSkeletonLayout(type) {
        var container = document.createElement('div');
        container.className = 'space-y-6';

        // タイトル行
        container.appendChild(createSkeletonCard('24px', '200px'));

        if (type === 'map') {
            container.appendChild(createSkeletonCard('500px'));
        } else if (type === 'table') {
            container.appendChild(createSkeletonCard('56px'));
            container.appendChild(createSkeletonCard('400px'));
        } else {
            // デフォルト: KPIカード4枚 + チャート2枚
            var stats = document.createElement('div');
            stats.className = 'grid-stats';
            for (var i = 0; i < 4; i++) stats.appendChild(createSkeletonCard('88px'));
            container.appendChild(stats);

            var charts = document.createElement('div');
            charts.className = 'grid-charts';
            for (var j = 0; j < 2; j++) charts.appendChild(createSkeletonCard('360px'));
            container.appendChild(charts);
        }
        return container;
    }

    var SKELETON_MAP = {
        '/tab/jobmap': 'map',
        '/tab/competitive': 'table'
    };

    // スケルトン表示（外部から呼び出し可能）
    window.showTabSkeleton = function(tabUrl) {
        var content = document.getElementById('content');
        if (!content) return;
        // 既存コンテンツをクリア
        while (content.firstChild) content.removeChild(content.firstChild);
        var type = SKELETON_MAP[tabUrl] || 'default';
        content.appendChild(createSkeletonLayout(type));
    };

    // ===== Phase 2: クライアントサイドキャッシュ =====

    var tabCache = {};
    var CACHE_TTL_MS = 5 * 60 * 1000; // 5分
    var CACHE_EXCLUDED = ['/tab/jobmap']; // 地図タブはキャッシュ除外

    function getFilterKey() {
        var jt = document.getElementById('job-type-select');
        var pref = document.getElementById('pref-select');
        var muni = document.getElementById('muni-select');
        return (jt ? jt.value : '') + '|' + (pref ? pref.value : '') + '|' + (muni ? muni.value : '');
    }

    function makeCacheKey(url) {
        return url + '::' + getFilterKey();
    }

    function isTabUrl(url) {
        return url && url.indexOf('/tab/') === 0;
    }

    function isCacheExcluded(url) {
        for (var i = 0; i < CACHE_EXCLUDED.length; i++) {
            if (CACHE_EXCLUDED[i] === url) return true;
        }
        return false;
    }

    // EChartsインスタンスを安全に破棄（メモリリーク防止）
    function disposeAllCharts(container) {
        if (typeof echarts === 'undefined') return;
        var elems = container.querySelectorAll('.echart');
        for (var i = 0; i < elems.length; i++) {
            var inst = echarts.getInstanceByDom(elems[i]);
            if (inst) { try { inst.dispose(); } catch(e) { /* 無視 */ } }
        }
    }

    // キャッシュヒット時: リクエストキャンセルしてキャッシュから復元
    document.body.addEventListener('htmx:configRequest', function(evt) {
        var url = evt.detail.path;
        if (!isTabUrl(url) || isCacheExcluded(url)) return;

        var key = makeCacheKey(url);
        var entry = tabCache[key];
        if (entry && (Date.now() - entry.timestamp) < CACHE_TTL_MS) {
            evt.preventDefault();

            var target = document.getElementById('content');
            if (target) {
                disposeAllCharts(target);
                // サーバー生成の信頼済みHTMLをDOMParserで安全にパース
                var parser = new DOMParser();
                var doc = parser.parseFromString(entry.html, 'text/html');
                while (target.firstChild) target.removeChild(target.firstChild);
                var nodes = doc.body.childNodes;
                while (nodes.length > 0) {
                    target.appendChild(document.adoptNode(nodes[0]));
                }

                // ECharts/Leaflet再初期化をトリガー
                var settleEvt = new CustomEvent('htmx:afterSettle', {
                    bubbles: true,
                    detail: { target: target }
                });
                target.dispatchEvent(settleEvt);
            }
        }
    });

    // サーバーレスポンスをキャッシュに保存（Leaflet初期化前の生HTML）
    document.body.addEventListener('htmx:beforeSwap', function(evt) {
        var serverResponse = evt.detail.serverResponse;
        if (!serverResponse) return;

        var activeBtn = document.querySelector('.tab-btn.active');
        var url = activeBtn ? activeBtn.getAttribute('hx-get') : null;
        if (!isTabUrl(url) || isCacheExcluded(url)) return;

        var key = makeCacheKey(url);
        tabCache[key] = {
            html: serverResponse,
            timestamp: Date.now()
        };
    });

    // キャッシュ全クリア（フィルタ変更時に呼び出し）
    window.clearTabCache = function() {
        tabCache = {};
    };
})();
