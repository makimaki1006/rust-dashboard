/**
 * export.js - 印刷 + チャート画像エクスポート
 */
(function() {
    'use strict';

    // 印刷実行
    window.printCurrentTab = function() {
        window.print();
    };

    // 全チャートを画像としてダウンロード（ZIP不要、個別ダウンロード）
    window.downloadCharts = function() {
        if (typeof echarts === 'undefined') return;

        var charts = document.querySelectorAll('#content .echart[data-chart-config]');
        if (charts.length === 0) {
            alert('ダウンロード可能なチャートがありません');
            return;
        }

        // タブ名取得
        var activeBtn = document.querySelector('.tab-btn.active');
        var tabName = activeBtn ? activeBtn.textContent.trim().replace(/[^\w\u3000-\u9fff]/g, '') : 'chart';

        charts.forEach(function(el, idx) {
            var instance = echarts.getInstanceByDom(el);
            if (!instance) return;

            try {
                var url = instance.getDataURL({
                    type: 'png',
                    pixelRatio: 2,
                    backgroundColor: '#0d1525'
                });

                var a = document.createElement('a');
                a.href = url;
                a.download = tabName + '_chart_' + (idx + 1) + '.png';
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
            } catch (e) {
                console.warn('[export.js] チャートダウンロードエラー:', e);
            }
        });
    };

    // HTMXコンテンツ切替後にエクスポートバーを自動挿入
    function injectExportBar(container) {
        if (!container) container = document;
        // 既にバーがあれば何もしない
        if (container.querySelector && container.querySelector('.print-export-bar')) return;

        // コンテンツ先頭のh2を探し、その横にバーを挿入
        var heading = container.querySelector('#content > div > h2, #content > div > div > h2');
        if (!heading) {
            // h2が無い場合は#contentの最初の子要素の先頭
            var firstChild = document.querySelector('#content > div');
            if (firstChild) heading = firstChild.querySelector('h2');
        }
        if (!heading) return;

        // 既にflex containerにバーがあれば何もしない
        var parent = heading.parentElement;
        if (parent.querySelector('.print-export-bar')) return;

        // h2をflex containerでラップするか、既にflexなら追加
        var bar = document.createElement('div');
        bar.className = 'print-export-bar';
        bar.innerHTML = '<button class="btn-export" onclick="printCurrentTab()" title="印刷 / PDF保存">' +
            '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M6 9V2h12v7"/><path d="M6 18H4a2 2 0 01-2-2v-5a2 2 0 012-2h16a2 2 0 012 2v5a2 2 0 01-2 2h-2"/><rect x="6" y="14" width="12" height="8"/></svg>' +
            '印刷</button>' +
            '<button class="btn-export" onclick="downloadCharts()" title="チャートを画像でダウンロード">' +
            '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>' +
            '画像保存</button>';

        // h2の横に配置するためflex化
        if (!parent.style.display || parent.style.display !== 'flex') {
            // h2とバーをflex containerでラップ
            var wrapper = document.createElement('div');
            wrapper.style.cssText = 'display:flex;align-items:center;justify-content:space-between;width:100%;';
            heading.parentNode.insertBefore(wrapper, heading);
            wrapper.appendChild(heading);
            wrapper.appendChild(bar);
        } else {
            parent.appendChild(bar);
        }
    }

    // HTMXコンテンツ切替後に自動挿入
    document.body.addEventListener('htmx:afterSettle', function(evt) {
        setTimeout(function() { injectExportBar(evt.detail.target); }, 50);
    });

    // 初回ロード時
    document.addEventListener('DOMContentLoaded', function() {
        setTimeout(function() { injectExportBar(document); }, 500);
    });
})();
