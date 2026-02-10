// HTMX afterSwap: ECharts/Leaflet の初期化
document.addEventListener('htmx:afterSwap', function(evt) {
    initECharts(evt.target);
    initLeafletMaps(evt.target);
});

// ウィンドウリサイズ時にEChartsをリサイズ
window.addEventListener('resize', function() {
    document.querySelectorAll('.echart').forEach(function(el) {
        var chart = echarts.getInstanceByDom(el);
        if (chart) chart.resize();
    });
});
