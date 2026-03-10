/**
 * postingmap.js — 地図分析（Tab 6）
 * 求人マーカー + 求職者分布 + ハイブリッドモード
 * Leaflet 標準ピンマーカー + 横スクロール詳細カード + ピン留め + 給与統計
 * + チェックボックス表示制御 + 地域分析ダッシュボード + リサイズハンドル
 * + 求職者バブルマーカー + GeoJSONポリゴン + フロー線
 *
 * セキュリティ: ピンカードの表示内容は escapeHtml() / textContent でサニタイズ。
 * 詳細カードHTMLはRustサーバー側でescape_html()処理済み。
 * 凡例コンテンツはtextContentを使用。
 * seeker-detail HTMLはRustサーバー側でescape_html()処理済み。
 */
var postingMap = (function() {
  var map = null;
  var markerGroup = null;
  var allMarkers = [];
  var activeDetailMarker = null;
  var pinnedCards = [];
  var connectionSvg = null;
  var initialized = false;
  var detailJsonCache = {};
  var regionSectionsLoaded = {};
  var lastSearchMuni = '';
  var lastSearchPref = '';

  // ビューポート連動（V2バックポート）
  var viewportEnabled = false;
  var viewportTimer = null;

  // 表示モード管理
  var currentViewMode = 'postings';

  // 求職者レイヤー
  var seekerGroup = null;
  var geojsonLayer = null;
  var flowGroup = null;
  var seekerData = null;

  // GAS準拠: 標準ピンアイコン
  var defaultIcon = L.icon({
    iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-blue.png',
    shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
    iconSize: [25, 41], iconAnchor: [12, 41], popupAnchor: [1, -34], shadowSize: [41, 41]
  });
  var detailIcon = L.icon({
    iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-orange.png',
    shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
    iconSize: [25, 41], iconAnchor: [12, 41], popupAnchor: [1, -34], shadowSize: [41, 41]
  });
  var pinnedIcon = L.icon({
    iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-red.png',
    shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
    iconSize: [25, 41], iconAnchor: [12, 41], popupAnchor: [1, -34], shadowSize: [41, 41]
  });

  function ensureInit() { if (!initialized || !map) init(); }

  function init() {
    if (initialized && map) { map.invalidateSize(); return; }
    var el = document.getElementById('jm-map');
    if (!el) return;
    if (el.offsetHeight === 0) el.style.minHeight = '400px';
    map = L.map('jm-map', { center: [36.5, 137.0], zoom: 6, zoomControl: true });
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap', maxZoom: 18
    }).addTo(map);
    connectionSvg = document.getElementById('jm-connection-svg');
    markerGroup = L.layerGroup().addTo(map);
    initialized = true;
    ['jm-pref','jm-muni','jm-radius','jm-emp','jm-salary-type'].forEach(function(id) {
      var e = document.getElementById(id);
      if (e) e.addEventListener('keydown', function(ev) { if (ev.key === 'Enter') search(); });
    });
    map.on('moveend', onViewportChange);
    initResizeHandle();
    updateUIForMode();
  }

  function initResizeHandle() {
    var handle = document.getElementById('jm-resize-handle');
    if (!handle) return;
    var isDragging = false;
    var mainContainer = document.getElementById('jm-main-container');
    var panel = document.getElementById('jm-details-panel');
    handle.addEventListener('mousedown', function(e) {
      isDragging = true; e.preventDefault();
      document.body.style.cursor = 'col-resize'; document.body.style.userSelect = 'none';
    });
    document.addEventListener('mousemove', function(e) {
      if (!isDragging || !mainContainer || !panel) return;
      var rect = mainContainer.getBoundingClientRect();
      var w = Math.max(250, Math.min(rect.right - e.clientX, rect.width * 0.6));
      panel.style.width = w + 'px';
      if (map) map.invalidateSize();
    });
    document.addEventListener('mouseup', function() {
      if (isDragging) {
        isDragging = false; document.body.style.cursor = ''; document.body.style.userSelect = '';
        if (map) map.invalidateSize();
      }
    });
  }

  // ===== 表示モード切替 =====

  function switchViewMode(mode) {
    currentViewMode = mode;
    updateUIForMode();
    clearSeekerLayers();
    if (mode === 'seekers' || mode === 'hybrid') {
      var pref = document.getElementById('jm-pref').value;
      if (pref) loadSeekerData(pref, lastSearchMuni || '');
    }
    if (mode === 'postings') {
      if (markerGroup && map && !map.hasLayer(markerGroup)) map.addLayer(markerGroup);
    } else if (mode === 'seekers') {
      if (markerGroup && map && map.hasLayer(markerGroup)) map.removeLayer(markerGroup);
    } else {
      if (markerGroup && map && !map.hasLayer(markerGroup)) map.addLayer(markerGroup);
    }
    updateLegend();
  }

  function updateUIForMode() {
    var mode = currentViewMode;
    var layerCtrl = document.getElementById('jm-layer-controls');
    var pinFieldsWrap = document.getElementById('jm-pin-fields-wrap');
    var radiusWrap = document.getElementById('jm-radius-wrap');
    var empWrap = document.getElementById('jm-emp-wrap');
    var salaryWrap = document.getElementById('jm-salary-type-wrap');
    var legend = document.getElementById('jm-legend');
    if (layerCtrl) { layerCtrl.classList.toggle('hidden', mode === 'postings'); }
    if (pinFieldsWrap) { pinFieldsWrap.classList.toggle('hidden', mode === 'seekers'); }
    [radiusWrap, empWrap, salaryWrap].forEach(function(el) {
      if (el) el.style.display = (mode === 'seekers') ? 'none' : '';
    });
    if (legend) { legend.classList.toggle('hidden', mode === 'postings'); }
  }

  function updateLegend() {
    var content = document.getElementById('jm-legend-content');
    if (!content) return;
    while (content.firstChild) content.removeChild(content.firstChild);
    var items;
    if (currentViewMode === 'postings') {
      items = [['#3b82f6','求人マーカー（青=通常）'],['#f97316','選択中（橙）'],['#ef4444','ピン留め（赤）']];
    } else if (currentViewMode === 'seekers') {
      items = [['#667eea','求職者数（円サイズ=人数）'],['#60a5fa','フロー線（居住→希望勤務地）'],['','ポリゴン色 = 求職者数']];
    } else {
      items = [['#3b82f6','求人マーカー'],['#667eea','求職者バブル'],['','ポリゴン色 = 求職者数']];
    }
    items.forEach(function(pair) {
      var div = document.createElement('div');
      div.className = 'text-gray-300';
      if (pair[0]) { div.style.color = pair[0]; div.textContent = '● ' + pair[1]; }
      else { div.textContent = pair[1]; }
      content.appendChild(div);
    });
  }

  // ===== ビューポート連動検索（V2バックポート） =====

  function onViewportChange() {
    if (!viewportEnabled || currentViewMode !== 'postings') return;
    if (viewportTimer) clearTimeout(viewportTimer);
    viewportTimer = setTimeout(loadViewportMarkers, 300);
  }

  function loadViewportMarkers() {
    if (!map) return;
    var bounds = map.getBounds();
    var emp = document.getElementById('jm-emp');
    var sal = document.getElementById('jm-salary-type');
    var params = new URLSearchParams({
      south: bounds.getSouth(),
      north: bounds.getNorth(),
      west: bounds.getWest(),
      east: bounds.getEast(),
      employment_type: emp ? emp.value : '',
      salary_type: sal ? sal.value : ''
    });
    fetch('/api/jobmap/markers?' + params.toString())
      .then(function(r) { return r.json(); })
      .then(function(data) {
        // ビューポート結果で更新（ただし地図のsetViewはしない）
        markerGroup.clearLayers(); allMarkers = [];
        var markers = data.markers || [];
        var totalAvail = data.totalAvailable || markers.length;
        var countText = markers.length + ' \u4ef6';
        if (totalAvail > markers.length) countText += ' / ' + totalAvail.toLocaleString() + ' \u4ef6\u4e2d';
        document.getElementById('jm-count').textContent = countText;
        markers.forEach(function(d) {
          var marker = L.marker([d.lat, d.lng], { icon: defaultIcon });
          var mi = { marker: marker, data: d, isPinned: false, isDetailActive: false };
          allMarkers.push(mi);
          marker.on('click', function() { onMarkerClick(mi); });
          var salText = formatYen(d.salaryMin) + ' \u301c ' + formatYen(d.salaryMax);
          marker.bindTooltip(escapeHtml(d.facility) + '\n' + escapeHtml(d.emp) + ' ' + salText, { direction: 'top', offset: [0, -8] });
          markerGroup.addLayer(marker);
        });
      })
      .catch(function(err) { console.warn('[postingmap] viewport load error:', err); });
  }

  // ===== 求職者レイヤー =====

  function clearSeekerLayers() {
    if (seekerGroup && map) { map.removeLayer(seekerGroup); seekerGroup = null; }
    if (geojsonLayer && map) { map.removeLayer(geojsonLayer); geojsonLayer = null; }
    if (flowGroup && map) { map.removeLayer(flowGroup); flowGroup = null; }
  }

  function loadSeekerData(pref, muni) {
    if (!pref) return;
    var params = new URLSearchParams({ prefecture: pref });
    if (muni) params.set('municipality', muni);
    fetch('/api/jobmap/seekers?' + params.toString())
      .then(function(r) { return r.json(); })
      .then(function(data) {
        seekerData = data;
        drawSeekerMarkers(data.markers || []);
        drawFlows(data.flows || []);
        loadGeoJSON(pref, data.choropleth || {});
        if (muni && (currentViewMode === 'seekers' || currentViewMode === 'hybrid')) {
          loadSeekerDetail(pref, muni);
        }
      })
      .catch(function(err) { console.warn('[postingmap] seeker data error:', err); });
  }

  function drawSeekerMarkers(markers) {
    if (seekerGroup && map) map.removeLayer(seekerGroup);
    seekerGroup = L.layerGroup();
    markers.forEach(function(m) {
      if (!m.lat || !m.lng) return;
      var radius = Math.max(4, Math.min(12, Math.sqrt(m.count) * 1.5));
      var circle = L.circleMarker([m.lat, m.lng], {
        radius: radius, fillColor: '#667eea', color: '#ffffff',
        weight: 1, opacity: 0.9, fillOpacity: 0.7
      });
      circle.bindTooltip(m.name + ': ' + m.count + '\u4eba', { direction: 'top' });
      circle.on('click', function() { onSeekerMarkerClick(m); });
      seekerGroup.addLayer(circle);
    });
    seekerGroup.addTo(map);
  }

  function loadGeoJSON(pref, choroplethStyles) {
    var prefCodes = {
      '\u5317\u6d77\u9053':'01','\u9752\u68ee\u770c':'02','\u5ca9\u624b\u770c':'03','\u5bae\u57ce\u770c':'04','\u79cb\u7530\u770c':'05',
      '\u5c71\u5f62\u770c':'06','\u798f\u5cf6\u770c':'07','\u8328\u57ce\u770c':'08','\u6803\u6728\u770c':'09','\u7fa4\u99ac\u770c':'10',
      '\u57fc\u7389\u770c':'11','\u5343\u8449\u770c':'12','\u6771\u4eac\u90fd':'13','\u795e\u5948\u5ddd\u770c':'14','\u65b0\u6f5f\u770c':'15',
      '\u5bcc\u5c71\u770c':'16','\u77f3\u5ddd\u770c':'17','\u798f\u4e95\u770c':'18','\u5c71\u68a8\u770c':'19','\u9577\u91ce\u770c':'20',
      '\u5c90\u961c\u770c':'21','\u9759\u5ca1\u770c':'22','\u611b\u77e5\u770c':'23','\u4e09\u91cd\u770c':'24','\u6ecb\u8cc0\u770c':'25',
      '\u4eac\u90fd\u5e9c':'26','\u5927\u962a\u5e9c':'27','\u5175\u5eab\u770c':'28','\u5948\u826f\u770c':'29','\u548c\u6b4c\u5c71\u770c':'30',
      '\u9ce5\u53d6\u770c':'31','\u5cf6\u6839\u770c':'32','\u5ca1\u5c71\u770c':'33','\u5e83\u5cf6\u770c':'34','\u5c71\u53e3\u770c':'35',
      '\u5fb3\u5cf6\u770c':'36','\u9999\u5ddd\u770c':'37','\u611b\u5a9b\u770c':'38','\u9ad8\u77e5\u770c':'39','\u798f\u5ca1\u770c':'40',
      '\u4f50\u8cc0\u770c':'41','\u9577\u5d0e\u770c':'42','\u718a\u672c\u770c':'43','\u5927\u5206\u770c':'44','\u5bae\u5d0e\u770c':'45',
      '\u9e7f\u5150\u5cf6\u770c':'46','\u6c96\u7e04\u770c':'47'
    };
    var romajiMap = {
      '01':'hokkaido','02':'aomori','03':'iwate','04':'miyagi','05':'akita',
      '06':'yamagata','07':'fukushima','08':'ibaraki','09':'tochigi','10':'gunma',
      '11':'saitama','12':'chiba','13':'tokyo','14':'kanagawa','15':'niigata',
      '16':'toyama','17':'ishikawa','18':'fukui','19':'yamanashi','20':'nagano',
      '21':'gifu','22':'shizuoka','23':'aichi','24':'mie','25':'shiga',
      '26':'kyoto','27':'osaka','28':'hyogo','29':'nara','30':'wakayama',
      '31':'tottori','32':'shimane','33':'okayama','34':'hiroshima','35':'yamaguchi',
      '36':'tokushima','37':'kagawa','38':'ehime','39':'kochi','40':'fukuoka',
      '41':'saga','42':'nagasaki','43':'kumamoto','44':'oita','45':'miyazaki',
      '46':'kagoshima','47':'okinawa'
    };
    var code = prefCodes[pref];
    if (!code) return;
    var url = '/api/geojson/' + code + '_' + romajiMap[code] + '.json';
    if (geojsonLayer && map) map.removeLayer(geojsonLayer);
    fetch(url).then(function(res) { return res.json(); }).then(function(geojson) {
      geojsonLayer = L.geoJSON(geojson, {
        style: function(feature) {
          var name = feature.properties.name || feature.properties.N03_004 || '';
          var s = choroplethStyles[name] || {};
          return { fillColor: s.fillColor || '#1e3a5f', weight: s.weight || 1, opacity: 1,
                   color: s.color || '#334155', fillOpacity: s.fillOpacity || 0.5 };
        },
        onEachFeature: function(feature, layer) {
          var name = feature.properties.name || feature.properties.N03_004 || '';
          layer.bindTooltip(name, { sticky: true });
          layer.on('click', function() { onSeekerMuniClick(name); });
        }
      }).addTo(map);
    }).catch(function(err) { console.warn('[postingmap] GeoJSON error:', err); });
  }

  function drawFlows(flows) {
    if (flowGroup && map) map.removeLayer(flowGroup);
    flowGroup = L.layerGroup();
    var showCb = document.getElementById('jm-show-flows');
    flows.forEach(function(f) {
      if (!f.from || !f.to) return;
      var w = Math.max(1, Math.min(f.weight || 2, 8));
      var line = L.polyline([f.from, f.to], { color: '#60a5fa', weight: w, opacity: 0.6 });
      if (f.count) line.bindTooltip(f.count + '\u4eba', { sticky: true });
      var midLat = (f.from[0] + f.to[0]) / 2, midLng = (f.from[1] + f.to[1]) / 2;
      var bearing = calcBearing(f.from, f.to);
      var arrowIcon = L.divIcon({
        html: '<div style="color:#60a5fa;font-size:12px;transform:rotate(' + bearing + 'deg);">&#9654;</div>',
        className: 'flow-arrow', iconSize: [12,12], iconAnchor: [6,6]
      });
      flowGroup.addLayer(line);
      flowGroup.addLayer(L.marker([midLat, midLng], { icon: arrowIcon, interactive: false }));
    });
    if (showCb && showCb.checked) flowGroup.addTo(map);
  }

  function calcBearing(from, to) {
    var dLng = (to[1] - from[1]) * Math.PI / 180;
    var lat1 = from[0] * Math.PI / 180, lat2 = to[0] * Math.PI / 180;
    var y = Math.sin(dLng) * Math.cos(lat2);
    var x = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLng);
    return (Math.atan2(y, x) * 180 / Math.PI + 360) % 360 - 90;
  }

  function onSeekerMarkerClick(m) {
    var pref = document.getElementById('jm-pref').value;
    if (pref && m.name) loadSeekerDetail(pref, m.name);
  }
  function onSeekerMuniClick(name) {
    var pref = document.getElementById('jm-pref').value;
    if (pref && name) loadSeekerDetail(pref, name);
  }

  function loadSeekerDetail(pref, muni) {
    var panel = document.getElementById('jm-details-panel');
    panel.classList.remove('hidden');
    var handle = document.getElementById('jm-resize-handle');
    if (handle) handle.classList.remove('hidden');
    var statsEl = document.getElementById('jm-seeker-stats');
    var contentEl = document.getElementById('jm-seeker-stats-content');
    statsEl.classList.remove('hidden');
    while (contentEl.firstChild) contentEl.removeChild(contentEl.firstChild);
    var loading = document.createElement('p');
    loading.className = 'text-gray-500 text-xs'; loading.textContent = '\u8aad\u307f\u8fbc\u307f\u4e2d...';
    contentEl.appendChild(loading);
    var params = new URLSearchParams({ prefecture: pref, municipality: muni });
    fetch('/api/jobmap/seeker-detail?' + params.toString())
      .then(function(r) { return r.text(); })
      .then(function(html) {
        while (contentEl.firstChild) contentEl.removeChild(contentEl.firstChild);
        // Rustサーバー側でescape_html()済みの安全なHTMLコンテンツ
        var parser = new DOMParser();
        var doc = parser.parseFromString(html, 'text/html');
        while (doc.body.firstChild) contentEl.appendChild(doc.body.firstChild);
        if (typeof initECharts === 'function') initECharts(contentEl);
      })
      .catch(function(err) {
        while (contentEl.firstChild) contentEl.removeChild(contentEl.firstChild);
        var errMsg = document.createElement('p');
        errMsg.className = 'text-red-400 text-xs';
        errMsg.textContent = '\u30a8\u30e9\u30fc: ' + (err.message || 'unknown');
        contentEl.appendChild(errMsg);
      });
  }

  function closeSeekerStats() {
    var el = document.getElementById('jm-seeker-stats');
    if (el) el.classList.add('hidden');
  }

  function toggleLayer(layerName, visible) {
    if (!map) return;
    if (layerName === 'polygons' && geojsonLayer) {
      if (visible) map.addLayer(geojsonLayer); else map.removeLayer(geojsonLayer);
    }
    if (layerName === 'seekerMarkers' && seekerGroup) {
      if (visible) map.addLayer(seekerGroup); else map.removeLayer(seekerGroup);
    }
    if (layerName === 'flows' && flowGroup) {
      if (visible) map.addLayer(flowGroup); else map.removeLayer(flowGroup);
    }
  }

  // ===== 求人検索 =====

  function search() {
    ensureInit();
    var pref = document.getElementById('jm-pref').value;
    if (!pref) { document.getElementById('jm-count').textContent = '\u90fd\u9053\u5e9c\u770c\u3092\u9078\u629e\u3057\u3066\u304f\u3060\u3055\u3044'; return; }
    if (currentViewMode === 'seekers') {
      document.getElementById('jm-count').textContent = '\u8aad\u307f\u8fbc\u307f\u4e2d...';
      lastSearchPref = pref;
      var m2 = document.getElementById('jm-muni').value || '';
      lastSearchMuni = m2;
      clearSeekerLayers();
      loadSeekerData(pref, m2);
      document.getElementById('jm-count').textContent = '\u6c42\u8077\u8005\u30c7\u30fc\u30bf\u8aad\u307f\u8fbc\u307f\u5b8c\u4e86';
      var rb = document.getElementById('jm-region-btn'); if (rb) rb.disabled = false;
      return;
    }
    var muni = document.getElementById('jm-muni').value;
    if (!muni) { document.getElementById('jm-count').textContent = '\u5e02\u533a\u753a\u6751\u3092\u9078\u629e\u3057\u3066\u304f\u3060\u3055\u3044'; return; }
    var radius = document.getElementById('jm-radius').value || '10';
    var emp = document.getElementById('jm-emp').value;
    var salaryType = document.getElementById('jm-salary-type').value;
    document.getElementById('jm-count').textContent = '\u691c\u7d22\u4e2d...';
    document.getElementById('jm-search-btn').disabled = true;
    lastSearchPref = pref; lastSearchMuni = muni;
    var params = new URLSearchParams({ prefecture: pref, municipality: muni, radius: radius,
      employment_type: emp, salary_type: salaryType });
    fetch('/api/jobmap/markers?' + params.toString())
      .then(function(r) { return r.json(); })
      .then(function(data) {
        drawMarkers(data);
        document.getElementById('jm-search-btn').disabled = false;
        var rb = document.getElementById('jm-region-btn'); if (rb) rb.disabled = false;
        if (currentViewMode === 'hybrid') loadSeekerData(pref, muni);
        // 初回検索後にビューポート連動を有効化
        setTimeout(function() { viewportEnabled = true; }, 500);
      })
      .catch(function(err) {
        document.getElementById('jm-count').textContent = '\u30a8\u30e9\u30fc: ' + err.message;
        document.getElementById('jm-search-btn').disabled = false;
      });
  }

  function drawMarkers(data) {
    clearPinnedCards(); markerGroup.clearLayers(); allMarkers = [];
    activeDetailMarker = null; detailJsonCache = {}; regionSectionsLoaded = {};
    var markers = data.markers || [];
    var totalAvail = data.totalAvailable || markers.length;
    var countText = markers.length + ' \u4ef6';
    if (totalAvail > markers.length) countText += ' / ' + totalAvail.toLocaleString() + ' \u4ef6\u4e2d';
    document.getElementById('jm-count').textContent = countText;
    if (markers.length === 0) { document.getElementById('jm-count').textContent = '\u8a72\u5f53\u306a\u3057'; return; }
    markers.forEach(function(d) {
      var marker = L.marker([d.lat, d.lng], { icon: defaultIcon });
      var mi = { marker: marker, data: d, isPinned: false, isDetailActive: false };
      allMarkers.push(mi);
      marker.on('click', function() { onMarkerClick(mi); });
      var sal = formatYen(d.salaryMin) + ' \u301c ' + formatYen(d.salaryMax);
      marker.bindTooltip(escapeHtml(d.facility) + '\n' + escapeHtml(d.emp) + ' ' + sal, { direction: 'top', offset: [0, -8] });
      markerGroup.addLayer(marker);
    });
    if (data.center) {
      var zoom = 12, r = parseFloat(document.getElementById('jm-radius').value) || 10;
      if (r <= 5) zoom = 14; else if (r <= 10) zoom = 13; else if (r <= 20) zoom = 12;
      else if (r <= 50) zoom = 10; else zoom = 9;
      map.setView([data.center.lat, data.center.lng], zoom);
    } else if (markers.length > 0) { map.fitBounds(markerGroup.getBounds(), { padding: [30, 30] }); }
  }

  function onMarkerClick(mi) {
    if (activeDetailMarker) {
      activeDetailMarker.marker.setIcon(activeDetailMarker.isPinned ? pinnedIcon : defaultIcon);
      activeDetailMarker.isDetailActive = false;
    }
    mi.marker.setIcon(detailIcon); mi.isDetailActive = true; activeDetailMarker = mi;
    var panel = document.getElementById('jm-details-panel'); panel.classList.remove('hidden');
    var handle = document.getElementById('jm-resize-handle'); if (handle) handle.classList.remove('hidden');
    fetch('/api/jobmap/detail/' + mi.data.id).then(function(r) { return r.text(); })
      .then(function(html) { addDetailCard(html, mi); });
  }

  function addDetailCard(serverRenderedHtml, mi) {
    var container = document.getElementById('jm-details-container');
    var ph = container.querySelector('p');
    if (ph) { while (container.firstChild) container.removeChild(container.firstChild); }
    while (container.children.length >= 4) container.removeChild(container.firstElementChild);
    var card = document.createElement('div');
    card.className = 'border border-gray-600 rounded-lg p-3 relative';
    card.style.cssText = 'background:#1e293b; flex-shrink:0; width:350px; min-width:300px;';
    var btnBar = document.createElement('div'); btnBar.className = 'flex justify-between items-center mb-2';
    var pinBtn = document.createElement('button');
    pinBtn.className = 'text-xs bg-blue-600 hover:bg-blue-500 text-white px-2 py-0.5 rounded';
    pinBtn.textContent = 'PIN'; pinBtn.addEventListener('click', function() { pinCard(pinBtn); });
    btnBar.appendChild(pinBtn);
    var closeBtn = document.createElement('button');
    closeBtn.className = 'text-gray-400 hover:text-white text-lg leading-none'; closeBtn.textContent = '\u00D7';
    closeBtn.addEventListener('click', function() { removeCard(closeBtn); });
    btnBar.appendChild(closeBtn); card.appendChild(btnBar);
    var content = document.createElement('div');
    // Rustサーバー側でescape_html()処理済みHTMLをDOM解析
    var parser = new DOMParser();
    var doc = parser.parseFromString(serverRenderedHtml, 'text/html');
    while (doc.body.firstChild) content.appendChild(doc.body.firstChild);
    card.appendChild(content); card._markerInfo = mi;
    container.appendChild(card); container.scrollLeft = container.scrollWidth;
  }

  function removeCard(btnEl) {
    var card = btnEl.closest('[style*="1e293b"]') || btnEl.parentElement.parentElement;
    var container = document.getElementById('jm-details-container');
    var mi = card._markerInfo; container.removeChild(card);
    if (mi && mi.isDetailActive) {
      mi.marker.setIcon(mi.isPinned ? pinnedIcon : defaultIcon);
      mi.isDetailActive = false; if (activeDetailMarker === mi) activeDetailMarker = null;
    }
    if (container.children.length === 0) {
      var p = document.createElement('p');
      p.className = 'text-gray-500 text-sm text-center py-4 flex-shrink-0 w-full';
      p.textContent = '\u30de\u30fc\u30ab\u30fc\u3092\u30af\u30ea\u30c3\u30af\u3067\u8a73\u7d30\u8868\u793a';
      container.appendChild(p);
    }
  }

  function fetchDetailJson(id) {
    if (detailJsonCache[id]) return Promise.resolve(detailJsonCache[id]);
    return fetch('/api/jobmap/detail-json/' + id).then(function(r) { return r.json(); })
      .then(function(data) { detailJsonCache[id] = data; return data; });
  }

  function getPinFields() {
    var fields = {}; var cbs = document.querySelectorAll('.jm-pin-field');
    for (var i = 0; i < cbs.length; i++) fields[cbs[i].getAttribute('data-field')] = cbs[i].checked;
    return fields;
  }

  function pinCard(btnEl) {
    var card = btnEl.closest('[style*="1e293b"]') || btnEl.parentElement.parentElement;
    var mi = card._markerInfo; if (!mi || mi.isPinned) return;
    fetchDetailJson(mi.data.id).then(function(detail) { buildPinnedCard(mi, detail); })
      .catch(function() { buildPinnedCardSimple(mi); });
  }

  function buildPinnedCard(mi, detail) {
    var d = mi.data, fields = getPinFields();
    var pc = document.createElement('div');
    pc.style.cssText = 'position:absolute;z-index:1000;background:rgba(255,255,255,0.95);border:2px solid #3b82f6;border-radius:6px;padding:5px 7px;font-size:11px;max-width:220px;min-width:120px;box-shadow:0 2px 8px rgba(0,0,0,0.3);cursor:move;user-select:none;line-height:1.3;color:#1e293b;';
    var cb = document.createElement('button');
    cb.style.cssText = 'position:absolute;top:1px;right:3px;border:none;background:transparent;font-size:12px;cursor:pointer;color:#3b82f6;font-weight:bold;';
    cb.textContent = '\u00D7'; cb.addEventListener('click', function(e) { e.stopPropagation(); removePinnedCard(pc); });
    pc.appendChild(cb);
    var info = document.createElement('div'); info.style.marginTop = '8px';
    if (fields.facility && detail.facility_name) addBoldLine(info, truncate(detail.facility_name, 25));
    if (fields.service && detail.service_type) addLine(info, detail.service_type);
    if (fields.access && detail.access) addLine(info, truncate(detail.access, 35));
    if (fields.emp && detail.employment_type) addLine(info, detail.employment_type);
    if (fields.salaryType && detail.salary_type) addLine(info, detail.salary_type);
    if (fields.salary && (detail.salary_min || detail.salary_max)) addLine(info, formatYen(detail.salary_min) + ' \u301c ' + formatYen(detail.salary_max));
    if (fields.salaryDetail && detail.salary_detail) addLine(info, truncate(detail.salary_detail, 40));
    if (fields.benefits && detail.benefits) addLine(info, truncate(detail.benefits, 40));
    if (fields.training && detail.education_training) addLine(info, truncate(detail.education_training, 40));
    if (fields.worktime && detail.working_hours) addLine(info, truncate(detail.working_hours, 40));
    if (fields.holiday && detail.holidays) addLine(info, truncate(detail.holidays, 30));
    if (fields.longHoliday && detail.special_holidays) addLine(info, truncate(detail.special_holidays, 30));
    if (fields.requirements && detail.requirements) addLine(info, truncate(detail.requirements, 40));
    if (fields.jobContent && detail.job_description) addLine(info, truncate(detail.job_description, 50));
    if (fields.jobPosition && detail.headline) addLine(info, truncate(detail.headline, 40));
    if (fields.tags && detail.tags) addLine(info, truncate(detail.tags, 35));
    if (fields.segment && detail.tier3_label_short) addLine(info, detail.tier3_label_short);
    if (fields.geocodeConf) {
      var conf = detail.geocode_confidence || 0, lv = detail.geocode_level || 0;
      var cl = conf >= 3 ? '\u25ce' : conf >= 2 ? '\u25cb' : conf >= 1 ? '\u25b3' : '\uff1f';
      var ll = lv >= 3 ? '\u8857\u533a' : lv >= 2 ? '\u5927\u5b57' : lv >= 1 ? '\u5e02\u533a\u753a\u6751' : '\u4e0d\u660e';
      addLine(info, '\u7cbe\u5ea6: ' + cl + '/' + ll);
    }
    if (fields.annualIncome && detail.salary_min > 0 && detail.salary_type === '\u6708\u7d66') {
      addLine(info, '\u60f3\u5b9a\u5e74\u53ce: ' + formatYen(detail.salary_min * 12) + '\u301c');
    }
    if (info.childNodes.length === 0) addLine(info, '\u8868\u793a\u9805\u76ee\u304c\u9078\u629e\u3055\u308c\u3066\u3044\u307e\u305b\u3093');
    pc.appendChild(info);
    var pt = map.latLngToContainerPoint([d.lat, d.lng]);
    pc.style.left = (pt.x + 20) + 'px'; pc.style.top = (pt.y - 20) + 'px';
    document.getElementById('jm-map-container').appendChild(pc);
    var cd = { element: pc, markerLat: d.lat, markerLng: d.lng, line: null, markerInfo: mi, data: d };
    pinnedCards.push(cd); mi.isPinned = true; mi.marker.setIcon(pinnedIcon);
    makeDraggable(pc, cd); updateConnectionLine(cd);
    if (pinnedCards.length === 1) map.on('move zoom', updateAllPinnedCards);
    updatePinnedStats();
  }

  function buildPinnedCardSimple(mi) {
    var d = mi.data;
    var pc = document.createElement('div');
    pc.style.cssText = 'position:absolute;z-index:1000;background:rgba(255,255,255,0.95);border:2px solid #3b82f6;border-radius:6px;padding:5px 7px;font-size:11px;max-width:180px;min-width:100px;box-shadow:0 2px 8px rgba(0,0,0,0.3);cursor:move;user-select:none;line-height:1.3;color:#1e293b;';
    var cb = document.createElement('button');
    cb.style.cssText = 'position:absolute;top:1px;right:3px;border:none;background:transparent;font-size:12px;cursor:pointer;color:#3b82f6;font-weight:bold;';
    cb.textContent = '\u00D7'; cb.addEventListener('click', function(e) { e.stopPropagation(); removePinnedCard(pc); });
    pc.appendChild(cb);
    var info = document.createElement('div'); info.style.marginTop = '8px';
    var fb = document.createElement('b'); fb.textContent = (d.facility || '').substring(0, 20); info.appendChild(fb);
    info.appendChild(document.createElement('br')); info.appendChild(document.createTextNode(d.emp || ''));
    info.appendChild(document.createElement('br')); info.appendChild(document.createTextNode(formatYen(d.salaryMin) + ' \u301c ' + formatYen(d.salaryMax)));
    pc.appendChild(info);
    var pt = map.latLngToContainerPoint([d.lat, d.lng]);
    pc.style.left = (pt.x + 20) + 'px'; pc.style.top = (pt.y - 20) + 'px';
    document.getElementById('jm-map-container').appendChild(pc);
    var cd = { element: pc, markerLat: d.lat, markerLng: d.lng, line: null, markerInfo: mi, data: d };
    pinnedCards.push(cd); mi.isPinned = true; mi.marker.setIcon(pinnedIcon);
    makeDraggable(pc, cd); updateConnectionLine(cd);
    if (pinnedCards.length === 1) map.on('move zoom', updateAllPinnedCards);
    updatePinnedStats();
  }

  function addLine(p, t) {
    var s = document.createElement('div');
    s.style.cssText = 'font-size:10px;color:#374151;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:200px;';
    s.textContent = t; p.appendChild(s);
  }
  function addBoldLine(p, t) {
    var b = document.createElement('div');
    b.style.cssText = 'font-size:11px;font-weight:bold;color:#1e293b;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:200px;';
    b.textContent = t; p.appendChild(b);
  }
  function truncate(s, m) { if (!s) return ''; return s.length > m ? s.substring(0, m - 1) + '\u2026' : s; }

  function makeDraggable(el, cd) {
    var isDragging = false, off = {};
    el.addEventListener('mousedown', function(e) {
      if (e.target.tagName === 'BUTTON') return;
      isDragging = true; map.dragging.disable();
      var r = el.getBoundingClientRect(); off = { x: e.clientX - r.left, y: e.clientY - r.top };
      el.style.cursor = 'grabbing'; e.preventDefault();
    });
    document.addEventListener('mousemove', function(e) {
      if (!isDragging) return;
      var mr = document.getElementById('jm-map-container').getBoundingClientRect();
      el.style.left = Math.max(0, Math.min(e.clientX - mr.left - off.x, mr.width - el.offsetWidth)) + 'px';
      el.style.top = Math.max(0, Math.min(e.clientY - mr.top - off.y, mr.height - el.offsetHeight)) + 'px';
      updateConnectionLine(cd);
    });
    document.addEventListener('mouseup', function() {
      if (isDragging) { isDragging = false; el.style.cursor = 'move'; setTimeout(function() { map.dragging.enable(); }, 100); }
    });
  }

  function updateConnectionLine(cd) {
    var svg = connectionSvg; if (!svg || !map) return;
    var mp = map.latLngToContainerPoint([cd.markerLat, cd.markerLng]);
    var cx = parseFloat(cd.element.style.left) + cd.element.offsetWidth / 2;
    var cy = parseFloat(cd.element.style.top) + cd.element.offsetHeight / 2;
    if (cd.line && svg.contains(cd.line)) svg.removeChild(cd.line);
    var line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    line.setAttribute('x1', mp.x); line.setAttribute('y1', mp.y);
    line.setAttribute('x2', cx); line.setAttribute('y2', cy);
    line.setAttribute('stroke', '#3b82f6'); line.setAttribute('stroke-width', '2');
    line.setAttribute('stroke-dasharray', '5,5'); line.setAttribute('opacity', '0.8');
    svg.appendChild(line); cd.line = line;
  }
  function updateAllPinnedCards() { pinnedCards.forEach(function(cd) { updateConnectionLine(cd); }); }

  function removePinnedCard(el) {
    var idx = pinnedCards.findIndex(function(c) { return c.element === el; });
    if (idx === -1) return; var cd = pinnedCards[idx];
    if (cd.element.parentNode) cd.element.parentNode.removeChild(cd.element);
    if (cd.line && connectionSvg && connectionSvg.contains(cd.line)) connectionSvg.removeChild(cd.line);
    cd.markerInfo.isPinned = false;
    cd.markerInfo.marker.setIcon(cd.markerInfo.isDetailActive ? detailIcon : defaultIcon);
    pinnedCards.splice(idx, 1);
    if (pinnedCards.length === 0) map.off('move zoom', updateAllPinnedCards);
    updatePinnedStats();
  }
  function clearPinnedCards() { while (pinnedCards.length > 0) removePinnedCard(pinnedCards[0].element); }

  function updatePinnedStats() {
    var el = document.getElementById('jm-pinned-stats');
    if (pinnedCards.length === 0) { el.classList.add('hidden'); return; }
    el.classList.remove('hidden');
    var mins = [], maxs = [];
    pinnedCards.forEach(function(c) { if (c.data.salaryMin > 0) mins.push(c.data.salaryMin); if (c.data.salaryMax > 0) maxs.push(c.data.salaryMax); });
    document.getElementById('jm-stats-title').textContent = '\u30d4\u30f3\u6b62\u3081\u65bd\u8a2d\u306e\u7d66\u4e0e\u7d71\u8a08 (' + pinnedCards.length + '\u4ef6)';
    fetch('/api/jobmap/stats', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ salary_mins: mins, salary_maxs: maxs }) })
    .then(function(r) { return r.json(); })
    .then(function(s) {
      var ct = document.getElementById('jm-stats-content');
      while (ct.firstChild) ct.removeChild(ct.firstChild);
      [{ title: '\u7d66\u4e0e\u4e0b\u9650', avg: s.min_avg, med: s.min_median, mode: s.min_mode },
       { title: '\u7d66\u4e0e\u4e0a\u9650', avg: s.max_avg, med: s.max_median, mode: s.max_mode }].forEach(function(item) {
        var box = document.createElement('div'); box.className = 'bg-gray-700/50 rounded p-2';
        var t = document.createElement('div'); t.className = 'text-gray-400 text-xs mb-1'; t.textContent = item.title; box.appendChild(t);
        ['\u5e73\u5747: ','\u4e2d\u592e\u5024: ','\u6700\u983b\u5024: '].forEach(function(l, i) {
          var v = [item.avg, item.med, item.mode][i];
          var row = document.createElement('div'); row.appendChild(document.createTextNode(l));
          var sp = document.createElement('span'); sp.className = 'text-yellow-300'; sp.textContent = formatYen(v);
          row.appendChild(sp); box.appendChild(row);
        });
        ct.appendChild(box);
      });
    });
  }

  function toggleStats() {
    var ct = document.getElementById('jm-stats-content'), btn = document.getElementById('jm-stats-toggle');
    if (ct.style.display === 'none') { ct.style.display = ''; btn.textContent = '\u6298\u308a\u305f\u305f\u3080'; }
    else { ct.style.display = 'none'; btn.textContent = '\u958b\u304f'; }
  }

  function closePanel() {
    document.getElementById('jm-details-panel').classList.add('hidden');
    var h = document.getElementById('jm-resize-handle'); if (h) h.classList.add('hidden');
    var c = document.getElementById('jm-details-container');
    while (c.firstChild) c.removeChild(c.firstChild);
    var p = document.createElement('p'); p.className = 'text-gray-500 text-sm text-center py-4 flex-shrink-0 w-full';
    p.textContent = '\u30de\u30fc\u30ab\u30fc\u3092\u30af\u30ea\u30c3\u30af\u3067\u8a73\u7d30\u8868\u793a'; c.appendChild(p);
    closeRegionDashboard(); closeSeekerStats();
    if (activeDetailMarker) {
      activeDetailMarker.marker.setIcon(activeDetailMarker.isPinned ? pinnedIcon : defaultIcon);
      activeDetailMarker.isDetailActive = false; activeDetailMarker = null;
    }
    if (map) map.invalidateSize();
  }

  function togglePinFields() {
    var l = document.getElementById('jm-pin-fields-list'), a = document.getElementById('jm-pin-fields-arrow');
    if (l.classList.contains('hidden')) { l.classList.remove('hidden'); a.textContent = '\u25bc'; }
    else { l.classList.add('hidden'); a.textContent = '\u25b6'; }
  }

  function openRegionDashboard() {
    if (!lastSearchPref || !lastSearchMuni) return;
    var panel = document.getElementById('jm-details-panel'); panel.classList.remove('hidden');
    var h = document.getElementById('jm-resize-handle'); if (h) h.classList.remove('hidden');
    var db = document.getElementById('jm-region-dashboard'); db.classList.remove('hidden');
    document.getElementById('jm-region-title').textContent = '\ud83d\udcca ' + lastSearchMuni + ' \u5730\u57df\u5206\u6790';
    regionSectionsLoaded = {};
    ['summary','age_gender','posting_stats','segments'].forEach(function(s) {
      var ct = document.getElementById('jm-region-content-' + s);
      if (ct) { ct.classList.add('hidden'); while (ct.firstChild) ct.removeChild(ct.firstChild); }
      var ar = document.getElementById('jm-region-arrow-' + s); if (ar) ar.textContent = '\u25b6';
    });
    toggleRegionSection('summary');
  }

  function closeRegionDashboard() { var d = document.getElementById('jm-region-dashboard'); if (d) d.classList.add('hidden'); }

  function toggleRegionSection(section) {
    var ct = document.getElementById('jm-region-content-' + section);
    var ar = document.getElementById('jm-region-arrow-' + section);
    if (!ct) return;
    if (ct.classList.contains('hidden')) {
      ct.classList.remove('hidden'); if (ar) ar.textContent = '\u25bc';
      if (!regionSectionsLoaded[section]) loadRegionSection(section);
    } else { ct.classList.add('hidden'); if (ar) ar.textContent = '\u25b6'; }
  }

  function loadRegionSection(section) {
    var ct = document.getElementById('jm-region-content-' + section); if (!ct) return;
    while (ct.firstChild) ct.removeChild(ct.firstChild);
    var ld = document.createElement('p'); ld.className = 'text-gray-500 text-xs py-2';
    ld.textContent = '\u8aad\u307f\u8fbc\u307f\u4e2d...'; ct.appendChild(ld);
    var urlMap = { 'summary':'/api/jobmap/region/summary','age_gender':'/api/jobmap/region/age_gender',
      'posting_stats':'/api/jobmap/region/posting_stats','segments':'/api/jobmap/region/segments' };
    var url = urlMap[section]; if (!url) return;
    var params = new URLSearchParams({ prefecture: lastSearchPref, municipality: lastSearchMuni });
    fetch(url + '?' + params.toString()).then(function(r) { return r.text(); }).then(function(html) {
      // Rustサーバー側でescape_html()処理済みのHTMLをDOM解析
      var parser = new DOMParser(); var doc = parser.parseFromString(html, 'text/html');
      while (ct.firstChild) ct.removeChild(ct.firstChild);
      while (doc.body.firstChild) ct.appendChild(doc.body.firstChild);
      regionSectionsLoaded[section] = true;
    }).catch(function(err) {
      while (ct.firstChild) ct.removeChild(ct.firstChild);
      var em = document.createElement('p'); em.className = 'text-red-400 text-xs';
      em.textContent = '\u30a8\u30e9\u30fc: ' + (err.message || 'unknown'); ct.appendChild(em);
    });
  }

  function formatYen(n) { if (!n || n === 0) return '\u2212'; return '\u00A5' + n.toLocaleString(); }
  function escapeHtml(s) { if (!s) return ''; var d = document.createElement('div'); d.appendChild(document.createTextNode(s)); return d.textContent; }

  function invalidateSize() { if (map) map.invalidateSize(); }

  return {
    init: init, search: search, pinCard: pinCard, removeCard: removeCard,
    removePinnedCard: removePinnedCard, closePanel: closePanel, toggleStats: toggleStats,
    togglePinFields: togglePinFields, openRegionDashboard: openRegionDashboard,
    closeRegionDashboard: closeRegionDashboard, toggleRegionSection: toggleRegionSection,
    switchViewMode: switchViewMode, toggleLayer: toggleLayer, closeSeekerStats: closeSeekerStats,
    invalidateSize: invalidateSize
  };
})();
