/**
 * maps.js - Leaflet地図初期化・GeoJSON・マーカー・フロー管理
 *
 * talentmap.html の #talentmap-leaflet 要素を対象に、
 * data-* 属性からLeaflet地図を初期化する。
 *
 * 対応data属性:
 *   data-lat, data-lng, data-zoom       : 初期表示位置
 *   data-geojson-url                    : GeoJSONファイルのURL
 *   data-choropleth-styles              : GeoJSON feature別の塗り分けスタイル (JSON)
 *   data-markers                        : マーカー配列 (JSON)
 *   data-flows                          : フローライン配列 (JSON)
 *   data-mode                           : 表示モード (basic/inflow/balance/competing)
 *   data-selected-muni                  : 選択中の市区町村名
 */
(function() {
    'use strict';

    // 管理中の地図インスタンス
    var mapInstances = {};

    // タイルレイヤー（CartoDB Dark Matter: navy背景に合う）
    var TILE_URL = 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png';
    var TILE_ATTR = '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>';

    /**
     * Leaflet地図を初期化
     * @param {HTMLElement} el - .leaflet-map要素
     */
    function initMap(el) {
        var mapId = el.id || 'leaflet-map-' + Date.now();

        // 既存インスタンスを破棄
        if (mapInstances[mapId]) {
            mapInstances[mapId].remove();
            delete mapInstances[mapId];
        }

        var lat = parseFloat(el.getAttribute('data-lat')) || 36.5;
        var lng = parseFloat(el.getAttribute('data-lng')) || 137.0;
        var zoom = parseInt(el.getAttribute('data-zoom'), 10) || 6;

        var map = L.map(el, {
            center: [lat, lng],
            zoom: zoom,
            zoomControl: true,
            attributionControl: true
        });

        L.tileLayer(TILE_URL, {
            attribution: TILE_ATTR,
            maxZoom: 18,
            subdomains: 'abcd'
        }).addTo(map);

        mapInstances[mapId] = map;

        // GeoJSON読み込み
        var geojsonUrl = el.getAttribute('data-geojson-url');
        if (geojsonUrl) {
            loadGeoJSON(map, el, geojsonUrl);
        }

        // マーカー描画
        var markersJson = el.getAttribute('data-markers');
        if (markersJson) {
            addMarkers(map, markersJson);
        }

        // フローライン描画
        var flowsJson = el.getAttribute('data-flows');
        if (flowsJson) {
            addFlows(map, flowsJson);
        }

        // 地図コントロールのイベントバインド（talentmap用）
        bindMapControls(map, el);

        // 少し遅延してinvalidateSizeを呼ぶ（レイアウト完了後）
        setTimeout(function() {
            map.invalidateSize();
        }, 100);

        return map;
    }

    /**
     * GeoJSONを読み込んでコロプレスマップを描画
     */
    function loadGeoJSON(map, el, url) {
        var stylesStr = el.getAttribute('data-choropleth-styles');
        var styles = {};
        if (stylesStr) {
            try { styles = JSON.parse(stylesStr); } catch(e) { /* 空のまま */ }
        }

        var selectedMuni = el.getAttribute('data-selected-muni') || '';
        var geojsonLayer = null;

        fetch(url)
            .then(function(res) { return res.json(); })
            .then(function(geojson) {
                geojsonLayer = L.geoJSON(geojson, {
                    style: function(feature) {
                        var name = feature.properties.name || feature.properties.N03_004 || '';
                        var featureStyle = styles[name] || {};
                        var isSelected = name === selectedMuni;
                        return {
                            fillColor: featureStyle.fillColor || '#1e3a5f',
                            weight: isSelected ? 3 : 1,
                            opacity: 1,
                            color: isSelected ? '#60a5fa' : (featureStyle.borderColor || '#334155'),
                            fillOpacity: featureStyle.fillOpacity || 0.6
                        };
                    },
                    onEachFeature: function(feature, layer) {
                        var name = feature.properties.name || feature.properties.N03_004 || '';
                        var featureStyle = styles[name] || {};
                        var tooltip = featureStyle.tooltip || name;
                        if (tooltip) {
                            layer.bindTooltip(tooltip, {
                                sticky: true,
                                className: 'leaflet-tooltip-dark'
                            });
                        }
                        // クリックイベント: HTMXでサイドバー更新
                        layer.on('click', function() {
                            onMuniClick(map, el, name);
                        });
                    }
                }).addTo(map);

                // レイヤーを地図に紐付け（コントロールで制御）
                map._geojsonLayer = geojsonLayer;
            })
            .catch(function(err) {
                console.warn('[maps.js] GeoJSON読み込みエラー:', url, err.message);
            });
    }

    /**
     * マーカーを追加
     * markersJson: [{"lat":35.68,"lng":139.76,"label":"xxx","popup":"<b>xxx</b>","color":"#ff0000"}]
     */
    function addMarkers(map, markersJson) {
        var markers;
        try { markers = JSON.parse(markersJson); } catch(e) { return; }
        if (!Array.isArray(markers)) return;

        var markerGroup = L.layerGroup();
        markers.forEach(function(m) {
            if (m.lat == null || m.lng == null) return;

            var icon = L.divIcon({
                html: '<div style="width:10px;height:10px;border-radius:50%;background:'
                    + (m.color || '#60a5fa')
                    + ';border:2px solid rgba(255,255,255,0.8);"></div>',
                className: 'custom-marker',
                iconSize: [10, 10],
                iconAnchor: [5, 5]
            });

            var marker = L.marker([m.lat, m.lng], { icon: icon });

            if (m.popup) {
                marker.bindPopup(m.popup, { maxWidth: 280 });
            } else if (m.label) {
                marker.bindPopup(m.label);
            }

            if (m.label) {
                marker.bindTooltip(m.label, { permanent: false, direction: 'top' });
            }

            markerGroup.addLayer(marker);
        });

        markerGroup.addTo(map);
        map._markerGroup = markerGroup;
    }

    /**
     * フローラインを追加
     * flowsJson: [{"from":[lat,lng],"to":[lat,lng],"weight":5,"color":"#60a5fa","label":"xxx"}]
     */
    function addFlows(map, flowsJson) {
        var flows;
        try { flows = JSON.parse(flowsJson); } catch(e) { return; }
        if (!Array.isArray(flows)) return;

        var flowGroup = L.layerGroup();
        flows.forEach(function(f) {
            if (!f.from || !f.to) return;

            var line = L.polyline([f.from, f.to], {
                color: f.color || '#60a5fa',
                weight: Math.max(1, Math.min(f.weight || 2, 8)),
                opacity: 0.7,
                dashArray: f.dashed ? '6,4' : null
            });

            if (f.label) {
                line.bindTooltip(f.label, { sticky: true });
            }

            // 矢印（フロー方向）
            var midLat = (f.from[0] + f.to[0]) / 2;
            var midLng = (f.from[1] + f.to[1]) / 2;
            var arrowIcon = L.divIcon({
                html: '<div style="color:' + (f.color || '#60a5fa') + ';font-size:14px;transform:rotate('
                    + calcBearing(f.from, f.to) + 'deg);">&#9654;</div>',
                className: 'flow-arrow',
                iconSize: [14, 14],
                iconAnchor: [7, 7]
            });
            var arrowMarker = L.marker([midLat, midLng], { icon: arrowIcon, interactive: false });

            flowGroup.addLayer(line);
            flowGroup.addLayer(arrowMarker);
        });

        flowGroup.addTo(map);
        map._flowGroup = flowGroup;
    }

    /**
     * 2地点間の方位角を計算（度）
     */
    function calcBearing(from, to) {
        var dLng = (to[1] - from[1]) * Math.PI / 180;
        var lat1 = from[0] * Math.PI / 180;
        var lat2 = to[0] * Math.PI / 180;
        var y = Math.sin(dLng) * Math.cos(lat2);
        var x = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLng);
        var bearing = Math.atan2(y, x) * 180 / Math.PI;
        return (bearing + 360) % 360 - 90; // CSS rotateは右が0度
    }

    /**
     * 市区町村クリック時のハンドラ
     */
    function onMuniClick(map, el, muniName) {
        // フィルタセレクタの取得
        var workstyle = '';
        var age = '';
        var gender = '';
        var mode = 'basic';

        var wsEl = document.getElementById('tm-workstyle');
        var ageEl = document.getElementById('tm-age');
        var genderEl = document.getElementById('tm-gender');
        var modeEls = document.querySelectorAll('input[name="tm-mode"]');

        if (wsEl) workstyle = wsEl.value;
        if (ageEl) age = ageEl.value;
        if (genderEl) gender = genderEl.value;
        modeEls.forEach(function(r) { if (r.checked) mode = r.value; });

        var url = '/tab/talentmap?municipality=' + encodeURIComponent(muniName)
            + '&workstyle=' + encodeURIComponent(workstyle)
            + '&age=' + encodeURIComponent(age)
            + '&gender=' + encodeURIComponent(gender)
            + '&mode=' + encodeURIComponent(mode);

        htmx.ajax('GET', url, {target: '#content', swap: 'innerHTML'});
    }

    /**
     * 地図コントロール（ポリゴン/マーカー/フロー表示切替）をバインド
     */
    function bindMapControls(map, el) {
        // ポリゴン表示チェックボックス
        var polygonCb = document.getElementById('tm-polygons');
        if (polygonCb) {
            polygonCb.addEventListener('change', function() {
                if (map._geojsonLayer) {
                    if (this.checked) {
                        map.addLayer(map._geojsonLayer);
                    } else {
                        map.removeLayer(map._geojsonLayer);
                    }
                }
            });
        }

        // マーカー表示チェックボックス
        var markerCb = document.getElementById('tm-markers');
        if (markerCb) {
            markerCb.addEventListener('change', function() {
                if (map._markerGroup) {
                    if (this.checked) {
                        map.addLayer(map._markerGroup);
                    } else {
                        map.removeLayer(map._markerGroup);
                    }
                }
            });
        }

        // フロー表示チェックボックス
        var flowCb = document.getElementById('tm-flows');
        if (flowCb) {
            flowCb.addEventListener('change', function() {
                if (map._flowGroup) {
                    if (this.checked) {
                        map.addLayer(map._flowGroup);
                    } else {
                        map.removeLayer(map._flowGroup);
                    }
                }
            });
        }

        // フィルタ変更時のHTMXタブ再読み込み
        ['tm-workstyle', 'tm-age', 'tm-gender'].forEach(function(id) {
            var sel = document.getElementById(id);
            if (sel) {
                sel.addEventListener('change', function() {
                    reloadTalentmap();
                });
            }
        });

        // 表示モードラジオボタン
        document.querySelectorAll('input[name="tm-mode"]').forEach(function(r) {
            r.addEventListener('change', function() {
                reloadTalentmap();
            });
        });
    }

    /**
     * talentmapタブをフィルタ付きで再読み込み
     */
    function reloadTalentmap() {
        var workstyle = '';
        var age = '';
        var gender = '';
        var mode = 'basic';
        var municipality = '';

        var wsEl = document.getElementById('tm-workstyle');
        var ageEl = document.getElementById('tm-age');
        var genderEl = document.getElementById('tm-gender');
        var modeEls = document.querySelectorAll('input[name="tm-mode"]');

        if (wsEl) workstyle = wsEl.value;
        if (ageEl) age = ageEl.value;
        if (genderEl) gender = genderEl.value;
        modeEls.forEach(function(r) { if (r.checked) mode = r.value; });

        var mapEl = document.getElementById('talentmap-leaflet');
        if (mapEl) {
            municipality = mapEl.getAttribute('data-selected-muni') || '';
        }

        var url = '/tab/talentmap?workstyle=' + encodeURIComponent(workstyle)
            + '&age=' + encodeURIComponent(age)
            + '&gender=' + encodeURIComponent(gender)
            + '&mode=' + encodeURIComponent(mode)
            + '&municipality=' + encodeURIComponent(municipality);

        htmx.ajax('GET', url, {target: '#content', swap: 'innerHTML'});
    }

    // グローバル公開: app.jsから呼び出される
    window.initLeafletMaps = function(container) {
        if (!container) container = document;
        var maps = container.querySelectorAll('.leaflet-map');
        maps.forEach(function(el) {
            // 高さ0はスキップ
            if (el.offsetHeight === 0) return;
            initMap(el);
        });
    };

    // リサイズ用: app.jsから呼び出される
    window.resizeLeafletMaps = function() {
        Object.keys(mapInstances).forEach(function(id) {
            if (mapInstances[id]) {
                mapInstances[id].invalidateSize();
            }
        });
    };
})();
