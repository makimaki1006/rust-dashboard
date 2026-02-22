/**
 * postingmap.js — 求人地図（Tab 6）
 * GAS Map.html からの移植・改修版
 * Leaflet 標準ピンマーカー + 横スクロール詳細カード + ピン留め + 給与統計
 * + チェックボックス表示制御 + 地域分析ダッシュボード + リサイズハンドル
 *
 * セキュリティ: innerHTML使用箇所はサーバーサイドでescapeHTML済みデータのみ。
 * ピンカードの表示内容は escapeHtml() 関数でサニタイズ済み。
 * 詳細カードHTMLはRustサーバー側でescape_html()処理済み。
 */
var postingMap = (function() {
  var map = null;
  var markerGroup = null;
  var allMarkers = [];      // {marker, data, isPinned, isDetailActive}
  var activeDetailMarker = null;
  var pinnedCards = [];      // {element, markerLat, markerLng, line, markerInfo, data}
  var connectionSvg = null;
  var initialized = false;
  var detailJsonCache = {};  // id -> 詳細JSONキャッシュ
  var regionSectionsLoaded = {}; // セクション名 -> ロード済みフラグ
  var lastSearchMuni = '';   // 最後に検索した市区町村
  var lastSearchPref = '';   // 最後に検索した都道府県

  // GAS準拠: 標準ピンアイコン（leaflet-color-markers）
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

  function ensureInit() {
    if (!initialized || !map) {
      init();
    }
  }

  function init() {
    if (initialized && map) {
      map.invalidateSize();
      return;
    }
    var el = document.getElementById('jm-map');
    if (!el) return;
    // HTMX読み込み直後はoffsetHeightが0の場合がある → 最小高さを強制設定
    if (el.offsetHeight === 0) {
      el.style.minHeight = '400px';
    }

    map = L.map('jm-map', {
      center: [36.5, 137.0],
      zoom: 6,
      zoomControl: true
    });
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap',
      maxZoom: 18
    }).addTo(map);

    connectionSvg = document.getElementById('jm-connection-svg');
    markerGroup = L.layerGroup().addTo(map);
    initialized = true;

    // Enterキーで検索
    ['jm-pref', 'jm-muni', 'jm-radius', 'jm-emp', 'jm-salary-type'].forEach(function(id) {
      var el = document.getElementById(id);
      if (el) {
        el.addEventListener('keydown', function(e) {
          if (e.key === 'Enter') search();
        });
      }
    });

    // リサイズハンドル初期化
    initResizeHandle();
  }

  // --- リサイズハンドル ---
  function initResizeHandle() {
    var handle = document.getElementById('jm-resize-handle');
    if (!handle) return;
    var isDragging = false;
    var mainContainer = document.getElementById('jm-main-container');
    var panel = document.getElementById('jm-details-panel');

    handle.addEventListener('mousedown', function(e) {
      isDragging = true;
      e.preventDefault();
      document.body.style.cursor = 'col-resize';
      document.body.style.userSelect = 'none';
    });

    document.addEventListener('mousemove', function(e) {
      if (!isDragging || !mainContainer || !panel) return;
      var rect = mainContainer.getBoundingClientRect();
      var newPanelWidth = rect.right - e.clientX;
      newPanelWidth = Math.max(250, Math.min(newPanelWidth, rect.width * 0.6));
      panel.style.width = newPanelWidth + 'px';
      if (map) map.invalidateSize();
    });

    document.addEventListener('mouseup', function() {
      if (isDragging) {
        isDragging = false;
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
        if (map) map.invalidateSize();
      }
    });
  }

  function search() {
    // マップ未初期化の場合は遅延初期化
    ensureInit();

    var pref = document.getElementById('jm-pref').value;
    if (!pref) {
      document.getElementById('jm-count').textContent = '都道府県を選択してください';
      return;
    }
    var muni = document.getElementById('jm-muni').value;
    if (!muni) {
      document.getElementById('jm-count').textContent = '市区町村を選択してください';
      return;
    }
    var radius = document.getElementById('jm-radius').value || '10';
    var emp = document.getElementById('jm-emp').value;
    var salaryType = document.getElementById('jm-salary-type').value;

    document.getElementById('jm-count').textContent = '検索中...';
    document.getElementById('jm-search-btn').disabled = true;

    lastSearchPref = pref;
    lastSearchMuni = muni;

    var params = new URLSearchParams({
      prefecture: pref,
      municipality: muni,
      radius: radius,
      employment_type: emp,
      salary_type: salaryType
    });

    fetch('/api/jobmap/markers?' + params.toString())
      .then(function(r) { return r.json(); })
      .then(function(data) {
        drawMarkers(data);
        document.getElementById('jm-search-btn').disabled = false;
        // 地域分析ボタンを有効化
        var regionBtn = document.getElementById('jm-region-btn');
        if (regionBtn) regionBtn.disabled = false;
      })
      .catch(function(err) {
        document.getElementById('jm-count').textContent = 'エラー: ' + err.message;
        document.getElementById('jm-search-btn').disabled = false;
      });
  }

  function drawMarkers(data) {
    // 既存マーカーとピンをクリア
    clearPinnedCards();
    markerGroup.clearLayers();
    allMarkers = [];
    activeDetailMarker = null;
    detailJsonCache = {};
    regionSectionsLoaded = {};

    var markers = data.markers || [];
    document.getElementById('jm-count').textContent = markers.length + ' 件';

    if (markers.length === 0) {
      document.getElementById('jm-count').textContent = '該当なし';
      return;
    }

    markers.forEach(function(d) {
      var marker = L.marker([d.lat, d.lng], { icon: defaultIcon });
      var markerInfo = { marker: marker, data: d, isPinned: false, isDetailActive: false };
      allMarkers.push(markerInfo);

      marker.on('click', function() {
        onMarkerClick(markerInfo);
      });

      // ツールチップ（ホバー用の簡易情報）
      var salary = formatYen(d.salaryMin) + ' 〜 ' + formatYen(d.salaryMax);
      marker.bindTooltip(
        escapeHtml(d.facility) + '\n' + escapeHtml(d.emp) + ' ' + salary,
        { direction: 'top', offset: [0, -8] }
      );

      markerGroup.addLayer(marker);
    });

    // 中心にフィット
    if (data.center) {
      var zoom = 12;
      var radius = parseFloat(document.getElementById('jm-radius').value) || 10;
      if (radius <= 5) zoom = 14;
      else if (radius <= 10) zoom = 13;
      else if (radius <= 20) zoom = 12;
      else if (radius <= 50) zoom = 10;
      else zoom = 9;
      map.setView([data.center.lat, data.center.lng], zoom);
    } else if (markers.length > 0) {
      var bounds = markerGroup.getBounds();
      map.fitBounds(bounds, { padding: [30, 30] });
    }
  }

  function onMarkerClick(markerInfo) {
    // 前の選択を解除
    if (activeDetailMarker) {
      activeDetailMarker.marker.setIcon(activeDetailMarker.isPinned ? pinnedIcon : defaultIcon);
      activeDetailMarker.isDetailActive = false;
    }

    markerInfo.marker.setIcon(detailIcon);
    markerInfo.isDetailActive = true;
    activeDetailMarker = markerInfo;

    // 詳細パネル表示
    var panel = document.getElementById('jm-details-panel');
    panel.classList.remove('hidden');
    // リサイズハンドルも表示
    var handle = document.getElementById('jm-resize-handle');
    if (handle) handle.classList.remove('hidden');

    // 詳細カード取得（サーバーサイドでescape済みHTML）
    fetch('/api/jobmap/detail/' + markerInfo.data.id)
      .then(function(r) { return r.text(); })
      .then(function(html) {
        addDetailCard(html, markerInfo);
      });
  }

  function addDetailCard(serverRenderedHtml, markerInfo) {
    var container = document.getElementById('jm-details-container');
    // プレースホルダー削除
    var placeholder = container.querySelector('p');
    if (placeholder) {
      while (container.firstChild) container.removeChild(container.firstChild);
    }

    // 最大4枚
    while (container.children.length >= 4) {
      container.removeChild(container.firstElementChild);
    }

    var card = document.createElement('div');
    card.className = 'border border-gray-600 rounded-lg p-3 relative';
    card.style.cssText = 'background:#1e293b; flex-shrink:0; width:350px; min-width:300px;';

    // ボタンバー
    var btnBar = document.createElement('div');
    btnBar.className = 'flex justify-between items-center mb-2';

    var pinBtn = document.createElement('button');
    pinBtn.className = 'text-xs bg-blue-600 hover:bg-blue-500 text-white px-2 py-0.5 rounded';
    pinBtn.textContent = 'PIN';
    pinBtn.addEventListener('click', function() { pinCard(pinBtn); });
    btnBar.appendChild(pinBtn);

    var closeBtn = document.createElement('button');
    closeBtn.className = 'text-gray-400 hover:text-white text-lg leading-none';
    closeBtn.textContent = '\u00D7';
    closeBtn.addEventListener('click', function() { removeCard(closeBtn); });
    btnBar.appendChild(closeBtn);

    card.appendChild(btnBar);

    // サーバーサイドでescape_html()済みのHTMLコンテンツをDOM解析
    var content = document.createElement('div');
    // Rustサーバー側でXSS対策済みの安全なHTMLコンテンツ
    var parser = new DOMParser();
    var doc = parser.parseFromString(serverRenderedHtml, 'text/html');
    while (doc.body.firstChild) {
      content.appendChild(doc.body.firstChild);
    }
    card.appendChild(content);

    // markerInfo をカード要素に紐付け
    card._markerInfo = markerInfo;

    container.appendChild(card);
    // 横スクロールで新しいカードを表示
    container.scrollLeft = container.scrollWidth;
  }

  function removeCard(btnEl) {
    var card = btnEl.closest('[style*="1e293b"]') || btnEl.parentElement.parentElement;
    var container = document.getElementById('jm-details-container');
    var markerInfo = card._markerInfo;

    container.removeChild(card);

    if (markerInfo && markerInfo.isDetailActive) {
      markerInfo.marker.setIcon(markerInfo.isPinned ? pinnedIcon : defaultIcon);
      markerInfo.isDetailActive = false;
      if (activeDetailMarker === markerInfo) activeDetailMarker = null;
    }

    if (container.children.length === 0) {
      var p = document.createElement('p');
      p.className = 'text-gray-500 text-sm text-center py-4 flex-shrink-0 w-full';
      p.textContent = 'マーカーをクリックで詳細表示';
      container.appendChild(p);
    }
  }

  // --- 詳細JSONフェッチ（ピンカード用） ---
  function fetchDetailJson(id) {
    if (detailJsonCache[id]) return Promise.resolve(detailJsonCache[id]);
    return fetch('/api/jobmap/detail-json/' + id)
      .then(function(r) { return r.json(); })
      .then(function(data) { detailJsonCache[id] = data; return data; });
  }

  // --- チェックボックス状態取得 ---
  function getPinFields() {
    var fields = {};
    var checks = document.querySelectorAll('.jm-pin-field');
    for (var i = 0; i < checks.length; i++) {
      fields[checks[i].getAttribute('data-field')] = checks[i].checked;
    }
    return fields;
  }

  function pinCard(btnEl) {
    var card = btnEl.closest('[style*="1e293b"]') || btnEl.parentElement.parentElement;
    var markerInfo = card._markerInfo;
    if (!markerInfo || markerInfo.isPinned) return;

    var d = markerInfo.data;

    // 詳細JSONをフェッチしてピンカード作成
    fetchDetailJson(d.id).then(function(detail) {
      buildPinnedCard(markerInfo, detail);
    }).catch(function() {
      // フェッチ失敗時はマーカーデータのみで簡易ピンカード
      buildPinnedCardSimple(markerInfo);
    });
  }

  function buildPinnedCard(markerInfo, detail) {
    var d = markerInfo.data;
    var fields = getPinFields();

    var pinnedCard = document.createElement('div');
    pinnedCard.style.cssText = 'position:absolute;z-index:1000;background:rgba(255,255,255,0.95);border:2px solid #3b82f6;border-radius:6px;padding:5px 7px;font-size:11px;max-width:220px;min-width:120px;box-shadow:0 2px 8px rgba(0,0,0,0.3);cursor:move;user-select:none;line-height:1.3;color:#1e293b;';

    // 閉じるボタン
    var closeBtn = document.createElement('button');
    closeBtn.style.cssText = 'position:absolute;top:1px;right:3px;border:none;background:transparent;font-size:12px;cursor:pointer;color:#3b82f6;font-weight:bold;';
    closeBtn.textContent = '\u00D7';
    closeBtn.addEventListener('click', function(e) {
      e.stopPropagation();
      removePinnedCard(pinnedCard);
    });
    pinnedCard.appendChild(closeBtn);

    // ミニカード内容（textContentでXSS安全、チェックボックスで出し分け）
    var infoDiv = document.createElement('div');
    infoDiv.style.marginTop = '8px';

    if (fields.facility && detail.facility_name) addBoldLine(infoDiv, truncate(detail.facility_name, 25));
    if (fields.service && detail.service_type) addLine(infoDiv, detail.service_type);
    if (fields.access && detail.access) addLine(infoDiv, truncate(detail.access, 35));
    if (fields.emp && detail.employment_type) addLine(infoDiv, detail.employment_type);
    if (fields.salaryType && detail.salary_type) addLine(infoDiv, detail.salary_type);
    if (fields.salary && (detail.salary_min || detail.salary_max)) {
      addLine(infoDiv, formatYen(detail.salary_min) + ' 〜 ' + formatYen(detail.salary_max));
    }
    if (fields.salaryDetail && detail.salary_detail) addLine(infoDiv, truncate(detail.salary_detail, 40));
    if (fields.benefits && detail.benefits) addLine(infoDiv, truncate(detail.benefits, 40));
    if (fields.training && detail.education_training) addLine(infoDiv, truncate(detail.education_training, 40));
    if (fields.worktime && detail.working_hours) addLine(infoDiv, truncate(detail.working_hours, 40));
    if (fields.holiday && detail.holidays) addLine(infoDiv, truncate(detail.holidays, 30));
    if (fields.longHoliday && detail.special_holidays) addLine(infoDiv, truncate(detail.special_holidays, 30));
    if (fields.requirements && detail.requirements) addLine(infoDiv, truncate(detail.requirements, 40));
    if (fields.jobContent && detail.job_description) addLine(infoDiv, truncate(detail.job_description, 50));
    if (fields.jobPosition && detail.headline) addLine(infoDiv, truncate(detail.headline, 40));
    if (fields.tags && detail.tags) addLine(infoDiv, truncate(detail.tags, 35));
    if (fields.segment && detail.tier3_label_short) addLine(infoDiv, detail.tier3_label_short);

    // 何も表示項目がない場合
    if (infoDiv.childNodes.length === 0) {
      addLine(infoDiv, '表示項目が選択されていません');
    }

    pinnedCard.appendChild(infoDiv);

    // 位置設定
    var point = map.latLngToContainerPoint([d.lat, d.lng]);
    pinnedCard.style.left = (point.x + 20) + 'px';
    pinnedCard.style.top = (point.y - 20) + 'px';

    document.getElementById('jm-map-container').appendChild(pinnedCard);

    var cardData = {
      element: pinnedCard,
      markerLat: d.lat,
      markerLng: d.lng,
      line: null,
      markerInfo: markerInfo,
      data: d
    };
    pinnedCards.push(cardData);

    markerInfo.isPinned = true;
    markerInfo.marker.setIcon(pinnedIcon);

    makeDraggable(pinnedCard, cardData);
    updateConnectionLine(cardData);

    if (pinnedCards.length === 1) {
      map.on('move zoom', updateAllPinnedCards);
    }

    updatePinnedStats();
  }

  // フォールバック: detail-json取得失敗時の簡易ピンカード
  function buildPinnedCardSimple(markerInfo) {
    var d = markerInfo.data;

    var pinnedCard = document.createElement('div');
    pinnedCard.style.cssText = 'position:absolute;z-index:1000;background:rgba(255,255,255,0.95);border:2px solid #3b82f6;border-radius:6px;padding:5px 7px;font-size:11px;max-width:180px;min-width:100px;box-shadow:0 2px 8px rgba(0,0,0,0.3);cursor:move;user-select:none;line-height:1.3;color:#1e293b;';

    var closeBtn = document.createElement('button');
    closeBtn.style.cssText = 'position:absolute;top:1px;right:3px;border:none;background:transparent;font-size:12px;cursor:pointer;color:#3b82f6;font-weight:bold;';
    closeBtn.textContent = '\u00D7';
    closeBtn.addEventListener('click', function(e) {
      e.stopPropagation();
      removePinnedCard(pinnedCard);
    });
    pinnedCard.appendChild(closeBtn);

    var infoDiv = document.createElement('div');
    infoDiv.style.marginTop = '8px';
    var facilityB = document.createElement('b');
    facilityB.textContent = (d.facility || '').substring(0, 20);
    infoDiv.appendChild(facilityB);
    infoDiv.appendChild(document.createElement('br'));
    infoDiv.appendChild(document.createTextNode(d.emp || ''));
    infoDiv.appendChild(document.createElement('br'));
    infoDiv.appendChild(document.createTextNode(formatYen(d.salaryMin) + ' 〜 ' + formatYen(d.salaryMax)));
    pinnedCard.appendChild(infoDiv);

    var point = map.latLngToContainerPoint([d.lat, d.lng]);
    pinnedCard.style.left = (point.x + 20) + 'px';
    pinnedCard.style.top = (point.y - 20) + 'px';

    document.getElementById('jm-map-container').appendChild(pinnedCard);

    var cardData = {
      element: pinnedCard,
      markerLat: d.lat,
      markerLng: d.lng,
      line: null,
      markerInfo: markerInfo,
      data: d
    };
    pinnedCards.push(cardData);

    markerInfo.isPinned = true;
    markerInfo.marker.setIcon(pinnedIcon);

    makeDraggable(pinnedCard, cardData);
    updateConnectionLine(cardData);

    if (pinnedCards.length === 1) {
      map.on('move zoom', updateAllPinnedCards);
    }

    updatePinnedStats();
  }

  // テキスト行追加ヘルパー
  function addLine(parent, text) {
    var span = document.createElement('div');
    span.style.cssText = 'font-size:10px;color:#374151;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:200px;';
    span.textContent = text;
    parent.appendChild(span);
  }

  function addBoldLine(parent, text) {
    var b = document.createElement('div');
    b.style.cssText = 'font-size:11px;font-weight:bold;color:#1e293b;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:200px;';
    b.textContent = text;
    parent.appendChild(b);
  }

  function truncate(s, maxLen) {
    if (!s) return '';
    return s.length > maxLen ? s.substring(0, maxLen - 1) + '…' : s;
  }

  function makeDraggable(element, cardData) {
    var isDragging = false, dragOffset = {};

    element.addEventListener('mousedown', function(e) {
      if (e.target.tagName === 'BUTTON') return;
      isDragging = true;
      map.dragging.disable();
      var rect = element.getBoundingClientRect();
      dragOffset = { x: e.clientX - rect.left, y: e.clientY - rect.top };
      element.style.cursor = 'grabbing';
      e.preventDefault();
    });

    document.addEventListener('mousemove', function(e) {
      if (!isDragging) return;
      var mapRect = document.getElementById('jm-map-container').getBoundingClientRect();
      var x = e.clientX - mapRect.left - dragOffset.x;
      var y = e.clientY - mapRect.top - dragOffset.y;
      element.style.left = Math.max(0, Math.min(x, mapRect.width - element.offsetWidth)) + 'px';
      element.style.top = Math.max(0, Math.min(y, mapRect.height - element.offsetHeight)) + 'px';
      updateConnectionLine(cardData);
    });

    document.addEventListener('mouseup', function() {
      if (isDragging) {
        isDragging = false;
        element.style.cursor = 'move';
        setTimeout(function() { map.dragging.enable(); }, 100);
      }
    });
  }

  function updateConnectionLine(cardData) {
    var svg = connectionSvg;
    if (!svg || !map) return;

    var markerPoint = map.latLngToContainerPoint([cardData.markerLat, cardData.markerLng]);
    var el = cardData.element;
    var cx = parseFloat(el.style.left) + el.offsetWidth / 2;
    var cy = parseFloat(el.style.top) + el.offsetHeight / 2;

    if (cardData.line && svg.contains(cardData.line)) {
      svg.removeChild(cardData.line);
    }
    var line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    line.setAttribute('x1', markerPoint.x);
    line.setAttribute('y1', markerPoint.y);
    line.setAttribute('x2', cx);
    line.setAttribute('y2', cy);
    line.setAttribute('stroke', '#3b82f6');
    line.setAttribute('stroke-width', '2');
    line.setAttribute('stroke-dasharray', '5,5');
    line.setAttribute('opacity', '0.8');
    svg.appendChild(line);
    cardData.line = line;
  }

  function updateAllPinnedCards() {
    pinnedCards.forEach(function(cd) { updateConnectionLine(cd); });
  }

  function removePinnedCard(pinnedCardEl) {
    var idx = pinnedCards.findIndex(function(c) { return c.element === pinnedCardEl; });
    if (idx === -1) return;
    var cardData = pinnedCards[idx];

    if (cardData.element.parentNode) cardData.element.parentNode.removeChild(cardData.element);
    if (cardData.line && connectionSvg && connectionSvg.contains(cardData.line)) {
      connectionSvg.removeChild(cardData.line);
    }

    var mi = cardData.markerInfo;
    mi.isPinned = false;
    mi.marker.setIcon(mi.isDetailActive ? detailIcon : defaultIcon);

    pinnedCards.splice(idx, 1);
    if (pinnedCards.length === 0) map.off('move zoom', updateAllPinnedCards);

    updatePinnedStats();
  }

  function clearPinnedCards() {
    while (pinnedCards.length > 0) {
      removePinnedCard(pinnedCards[0].element);
    }
  }

  function updatePinnedStats() {
    var statsEl = document.getElementById('jm-pinned-stats');
    if (pinnedCards.length === 0) {
      statsEl.classList.add('hidden');
      return;
    }
    statsEl.classList.remove('hidden');

    var mins = [], maxs = [];
    pinnedCards.forEach(function(c) {
      if (c.data.salaryMin > 0) mins.push(c.data.salaryMin);
      if (c.data.salaryMax > 0) maxs.push(c.data.salaryMax);
    });

    document.getElementById('jm-stats-title').textContent =
      'ピン止め施設の給与統計 (' + pinnedCards.length + '件)';

    fetch('/api/jobmap/stats', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ salary_mins: mins, salary_maxs: maxs })
    })
    .then(function(r) { return r.json(); })
    .then(function(s) {
      var content = document.getElementById('jm-stats-content');
      while (content.firstChild) content.removeChild(content.firstChild);

      var labels = [
        { title: '給与下限', avg: s.min_avg, med: s.min_median, mode: s.min_mode },
        { title: '給与上限', avg: s.max_avg, med: s.max_median, mode: s.max_mode }
      ];
      labels.forEach(function(item) {
        var box = document.createElement('div');
        box.className = 'bg-gray-700/50 rounded p-2';
        var titleEl = document.createElement('div');
        titleEl.className = 'text-gray-400 text-xs mb-1';
        titleEl.textContent = item.title;
        box.appendChild(titleEl);

        ['平均: ', '中央値: ', '最頻値: '].forEach(function(label, i) {
          var val = [item.avg, item.med, item.mode][i];
          var row = document.createElement('div');
          row.appendChild(document.createTextNode(label));
          var span = document.createElement('span');
          span.className = 'text-yellow-300';
          span.textContent = formatYen(val);
          row.appendChild(span);
          box.appendChild(row);
        });

        content.appendChild(box);
      });
    });
  }

  function toggleStats() {
    var content = document.getElementById('jm-stats-content');
    var btn = document.getElementById('jm-stats-toggle');
    if (content.style.display === 'none') {
      content.style.display = '';
      btn.textContent = '折りたたむ';
    } else {
      content.style.display = 'none';
      btn.textContent = '開く';
    }
  }

  function closePanel() {
    document.getElementById('jm-details-panel').classList.add('hidden');
    var handle = document.getElementById('jm-resize-handle');
    if (handle) handle.classList.add('hidden');
    var container = document.getElementById('jm-details-container');
    while (container.firstChild) container.removeChild(container.firstChild);
    var p = document.createElement('p');
    p.className = 'text-gray-500 text-sm text-center py-4 flex-shrink-0 w-full';
    p.textContent = 'マーカーをクリックで詳細表示';
    container.appendChild(p);

    // 地域分析も閉じる
    closeRegionDashboard();

    if (activeDetailMarker) {
      activeDetailMarker.marker.setIcon(activeDetailMarker.isPinned ? pinnedIcon : defaultIcon);
      activeDetailMarker.isDetailActive = false;
      activeDetailMarker = null;
    }

    if (map) map.invalidateSize();
  }

  // --- チェックボックスパネル開閉 ---
  function togglePinFields() {
    var list = document.getElementById('jm-pin-fields-list');
    var arrow = document.getElementById('jm-pin-fields-arrow');
    if (list.classList.contains('hidden')) {
      list.classList.remove('hidden');
      arrow.textContent = '▼';
    } else {
      list.classList.add('hidden');
      arrow.textContent = '▶';
    }
  }

  // --- 地域分析ダッシュボード ---
  function openRegionDashboard() {
    if (!lastSearchPref || !lastSearchMuni) return;

    var panel = document.getElementById('jm-details-panel');
    panel.classList.remove('hidden');
    var handle = document.getElementById('jm-resize-handle');
    if (handle) handle.classList.remove('hidden');

    var dashboard = document.getElementById('jm-region-dashboard');
    dashboard.classList.remove('hidden');

    var title = document.getElementById('jm-region-title');
    title.textContent = '📊 ' + lastSearchMuni + ' 地域分析';

    // セクションをリセット
    regionSectionsLoaded = {};
    ['summary', 'age_gender', 'posting_stats', 'segments'].forEach(function(section) {
      var content = document.getElementById('jm-region-content-' + section);
      if (content) {
        content.classList.add('hidden');
        while (content.firstChild) content.removeChild(content.firstChild);
      }
      var arrow = document.getElementById('jm-region-arrow-' + section);
      if (arrow) arrow.textContent = '▶';
    });

    // サマリーは自動展開
    toggleRegionSection('summary');
  }

  function closeRegionDashboard() {
    var dashboard = document.getElementById('jm-region-dashboard');
    if (dashboard) dashboard.classList.add('hidden');
  }

  function toggleRegionSection(section) {
    var content = document.getElementById('jm-region-content-' + section);
    var arrow = document.getElementById('jm-region-arrow-' + section);
    if (!content) return;

    if (content.classList.contains('hidden')) {
      content.classList.remove('hidden');
      if (arrow) arrow.textContent = '▼';
      // まだロード済みでなければAPIを呼ぶ
      if (!regionSectionsLoaded[section]) {
        loadRegionSection(section);
      }
    } else {
      content.classList.add('hidden');
      if (arrow) arrow.textContent = '▶';
    }
  }

  function loadRegionSection(section) {
    var content = document.getElementById('jm-region-content-' + section);
    if (!content) return;

    var loadingMsg = document.createElement('p');
    loadingMsg.className = 'text-gray-500 text-xs py-2';
    loadingMsg.textContent = '読み込み中...';
    while (content.firstChild) content.removeChild(content.firstChild);
    content.appendChild(loadingMsg);

    var apiMap = {
      'summary': '/api/jobmap/region/summary',
      'age_gender': '/api/jobmap/region/age_gender',
      'posting_stats': '/api/jobmap/region/posting_stats',
      'segments': '/api/jobmap/region/segments'
    };

    var url = apiMap[section];
    if (!url) return;

    var params = new URLSearchParams({
      prefecture: lastSearchPref,
      municipality: lastSearchMuni
    });

    fetch(url + '?' + params.toString())
      .then(function(r) { return r.text(); })
      .then(function(html) {
        // Rustサーバー側でXSS対策済みの安全なHTMLコンテンツ
        var parser = new DOMParser();
        var doc = parser.parseFromString(html, 'text/html');
        while (content.firstChild) content.removeChild(content.firstChild);
        while (doc.body.firstChild) {
          content.appendChild(doc.body.firstChild);
        }
        regionSectionsLoaded[section] = true;
      })
      .catch(function(err) {
        while (content.firstChild) content.removeChild(content.firstChild);
        var errMsg = document.createElement('p');
        errMsg.className = 'text-red-400 text-xs';
        errMsg.textContent = 'エラー: ' + (err.message || 'unknown');
        content.appendChild(errMsg);
      });
  }

  // ユーティリティ
  function formatYen(n) {
    if (!n || n === 0) return '\u2212'; // −
    return '\u00A5' + n.toLocaleString(); // ¥
  }

  function escapeHtml(s) {
    if (!s) return '';
    var div = document.createElement('div');
    div.appendChild(document.createTextNode(s));
    return div.textContent;
  }

  // 公開API
  return {
    init: init,
    search: search,
    pinCard: pinCard,
    removeCard: removeCard,
    removePinnedCard: removePinnedCard,
    closePanel: closePanel,
    toggleStats: toggleStats,
    togglePinFields: togglePinFields,
    openRegionDashboard: openRegionDashboard,
    closeRegionDashboard: closeRegionDashboard,
    toggleRegionSection: toggleRegionSection
  };
})();
