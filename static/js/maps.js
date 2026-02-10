// Leaflet地図初期化ヘルパー

// 地図インスタンスを管理
var _mapInstances = {};

/**
 * コンテナ内のLeaflet地図を初期化
 */
function initLeafletMaps(container) {
    container.querySelectorAll('.leaflet-map').forEach(function(el) {
        var mapId = el.id;
        if (!mapId) return;

        // 既存インスタンスがあれば破棄
        if (_mapInstances[mapId]) {
            _mapInstances[mapId].remove();
            delete _mapInstances[mapId];
        }

        var lat = parseFloat(el.getAttribute('data-lat')) || 36.5;
        var lng = parseFloat(el.getAttribute('data-lng')) || 138.0;
        var zoom = parseInt(el.getAttribute('data-zoom')) || 7;

        var map = L.map(mapId).setView([lat, lng], zoom);

        // OpenStreetMap タイルレイヤー（ダーク）
        L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            attribution: '&copy; OpenStreetMap contributors, &copy; CARTO',
            maxZoom: 18
        }).addTo(map);

        _mapInstances[mapId] = map;

        // GeoJSONロード
        var geojsonUrl = el.getAttribute('data-geojson-url');
        if (geojsonUrl) {
            fetch(geojsonUrl)
                .then(function(resp) { return resp.json(); })
                .then(function(data) {
                    if (data && data.features) {
                        var styleStr = el.getAttribute('data-choropleth-styles');
                        var styles = styleStr ? JSON.parse(styleStr) : {};

                        L.geoJSON(data, {
                            style: function(feature) {
                                var name = feature.properties.N03_004 || feature.properties.name || '';
                                return styles[name] || {
                                    fillColor: '#3b82f6',
                                    weight: 1,
                                    opacity: 1,
                                    color: '#666',
                                    fillOpacity: 0.5
                                };
                            },
                            onEachFeature: function(feature, layer) {
                                var name = feature.properties.N03_004 || feature.properties.name || '';
                                if (name) {
                                    layer.bindTooltip(name, { sticky: true });
                                }
                            }
                        }).addTo(map);

                        // GeoJSONの範囲にフィット
                        map.invalidateSize();
                    }
                })
                .catch(function(err) {
                    console.error('GeoJSON load error:', err);
                });
        }

        // マーカーロード
        var markersUrl = el.getAttribute('data-markers-url');
        if (markersUrl) {
            fetch(markersUrl)
                .then(function(resp) { return resp.json(); })
                .then(function(markers) {
                    if (Array.isArray(markers)) {
                        markers.forEach(function(m) {
                            if (m.lat && m.lng) {
                                L.marker([m.lat, m.lng])
                                    .bindPopup(m.popup || m.name || '')
                                    .addTo(map);
                            }
                        });
                    }
                })
                .catch(function(err) {
                    console.error('Markers load error:', err);
                });
        }
    });
}
