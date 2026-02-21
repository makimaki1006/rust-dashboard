/**
 * postingmap.js — 求人地図（Tab 6）
 * GAS Map.html からの移植・改修版
 * Leaflet マーカー + 詳細カード + ピン留め + 給与統計
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

  // アイコン定義（divIconでダークテーマ対応）
  function makeIcon(color, size) {
    size = size || 10;
    return L.divIcon({
      html: '<div style="width:' + size + 'px;height:' + size + 'px;border-radius:50%;background:' + color + ';border:2px solid rgba(255,255,255,0.8);box-shadow:0 1px 4px rgba(0,0,0,0.5);"></div>',
      iconSize: [size, size],
      iconAnchor: [size/2, size/2]
    });
  }
  var defaultIcon = makeIcon('#60a5fa');      // 青
  var detailIcon = makeIcon('#f59e0b', 14);   // オレンジ
  var pinnedIcon = makeIcon('#ef4444', 14);   // 赤

  function init() {
    if (initialized && map) {
      map.invalidateSize();
      return;
    }
    var el = document.getElementById('jm-map');
    if (!el || el.offsetHeight === 0) return;

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
  }

  function search() {
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

      // ツールチップ（ホバー用の簡易情報）- textContentベースで安全
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
    card.style.background = '#1e293b';

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

    // サーバーサイドでescape_html()済みのHTMLコンテンツ
    var content = document.createElement('div');
    content.innerHTML = serverRenderedHtml; // Rustサーバー側でXSS対策済み
    card.appendChild(content);

    // markerInfo をカード要素に紐付け
    card._markerInfo = markerInfo;

    container.appendChild(card);
    container.scrollTop = container.scrollHeight;
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
      p.className = 'text-gray-500 text-sm text-center py-4';
      p.textContent = 'マーカーをクリックで詳細表示';
      container.appendChild(p);
    }
  }

  function pinCard(btnEl) {
    var card = btnEl.closest('[style*="1e293b"]') || btnEl.parentElement.parentElement;
    var markerInfo = card._markerInfo;
    if (!markerInfo || markerInfo.isPinned) return;

    var d = markerInfo.data;

    // ピンカード（地図上のミニカード）
    var pinnedCard = document.createElement('div');
    pinnedCard.style.cssText = 'position:absolute;z-index:1000;background:rgba(255,255,255,0.95);border:2px solid #3b82f6;border-radius:6px;padding:5px 7px;font-size:11px;max-width:180px;min-width:100px;box-shadow:0 2px 8px rgba(0,0,0,0.3);cursor:move;user-select:none;line-height:1.3;color:#1e293b;';

    // 閉じるボタン
    var closeBtn = document.createElement('button');
    closeBtn.style.cssText = 'position:absolute;top:1px;right:3px;border:none;background:transparent;font-size:12px;cursor:pointer;color:#3b82f6;font-weight:bold;';
    closeBtn.textContent = '\u00D7';
    closeBtn.addEventListener('click', function(e) {
      e.stopPropagation();
      removePinnedCard(pinnedCard);
    });
    pinnedCard.appendChild(closeBtn);

    // ミニカード内容（textContentでXSS安全）
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

    // サーバー側で統計計算
    fetch('/api/jobmap/stats', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ salary_mins: mins, salary_maxs: maxs })
    })
    .then(function(r) { return r.json(); })
    .then(function(s) {
      var content = document.getElementById('jm-stats-content');
      // DOMで安全に構築
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
    var container = document.getElementById('jm-details-container');
    while (container.firstChild) container.removeChild(container.firstChild);
    var p = document.createElement('p');
    p.className = 'text-gray-500 text-sm text-center py-4';
    p.textContent = 'マーカーをクリックで詳細表示';
    container.appendChild(p);

    if (activeDetailMarker) {
      activeDetailMarker.marker.setIcon(activeDetailMarker.isPinned ? pinnedIcon : defaultIcon);
      activeDetailMarker.isDetailActive = false;
      activeDetailMarker = null;
    }
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
    toggleStats: toggleStats
  };
})();
