/*global $:false, L:false */

/**
 * Provides access to the actual map
 *
 * @type {{map: null, markerCluster: L.MarkerClusterGroup, companyLayer: *, manualLayer: *, heat: null, init: socialMap.init, resetView: socialMap.resetView, zoomIn: socialMap.zoomIn, zoomOut: socialMap.zoomOut, clearCompanyLayer: socialMap.clearCompanyLayer, clearManualLayer: socialMap.clearManualLayer, showRoute: socialMap.mainReturn, panMapToMarker: socialMap.panMapToMarker, addMarker: socialMap.addMarker, removeMarker: socialMap.removeMarker, addHeatmapLayer: socialMap.addHeatmapLayer}}
 */
var socialMap = {
    map: null,
    markerCluster: new L.MarkerClusterGroup({
        iconCreateFunction: function (cluster) {
            return new L.DivIcon({
                iconSize: [30, 30],
                className: 'markerCluster',
                html: "<div class='markerClusterLabel'>" + cluster.getChildCount() + "</div>"
            });
        }
    }),
    companyLayer: L.layerGroup(),
    manualLayer: L.layerGroup(),
    trafficLayer: L.layerGroup(),
    heat: null,

    init: function () {
        if (this.map != null) {
            this.map.remove();
            sideBar.resetFilterSettings();
        }

        moment().locale();
        if (feedController.heatmap) {
            mapboxgl.accessToken = 'pk.eyJ1Ijoic2Q0bSIsImEiOiJjaWp6b2k4a2UwMDVvdzVrcGVlem9iYzI5In0.qOFCTfOMHkypCwDPRHuQVQ';
            this.map = new mapboxgl.Map({
                container: 'map-canvas', // container id
                style: 'mapbox://styles/sd4m/ciyn0gwdf00a62slapoz8eg7r', //hosted style id
                center: [52.5, 13.35], // starting position
                zoom: 11, // starting zoom
                minZoom: 0,
                maxZoom: 22,
                maxBounds: [[-2.98865807458, 45.3024876979], [23.0169958839, 56.983104153]]
            });
            this.map.dragRotate.disable();               // disable rotation
            this.map.touchZoomRotate.disableRotation();  // disable touch rotation
            sideBar.initSlider();
            sideBar.disableCompanySearch();
            this.map.on('sourcedata', function(e){
                sideBar.hideLoadingSpinner();
            });
        } else {
            sideBar.destroySlider();
            sideBar.hideLoadingSpinner();
            sideBar.enableCompanySearch();
            L.mapbox.accessToken = 'pk.eyJ1Ijoic2Q0bSIsImEiOiJjaWp6b2k4a2UwMDVvdzVrcGVlem9iYzI5In0.qOFCTfOMHkypCwDPRHuQVQ';
            this.map = L.mapbox.map('map-canvas', null, {
                minZoom: 0,
                maxZoom: 22,
                maxBounds: [[45.3024876979, -2.98865807458], [56.983104153, 23.0169958839]],
                zoomControl: false
            }).setView([52.5, 13.35], 11);
            L.mapbox.styleLayer('mapbox://styles/sd4m/ciyn0gwdf00a62slapoz8eg7r').addTo(this.map);
            socialMap.companyLayer.addTo(socialMap.map);
            socialMap.manualLayer.addTo(socialMap.map);
            socialMap.trafficLayer.addTo(socialMap.map);
        }
        // close the side menu when the map is clicked
        this.map.on("click", function() {
            $(".c-menu__close").click()
        })
    },

    /**
     * Centers the map and clears the manualLayer
     */
    resetView: function () {
        if (feedController.heatmap) {
            socialMap.map.jumpTo({
                center: [52.5, 13.35],
                zoom: 11
            });
        } else {
            socialMap.map.setView({
                lat: 52.5,
                lng: 13.35
            }, 11);
            this.clearManualLayer();
        }

    },

    zoomIn: function () {
        socialMap.map.zoomIn();
    },

    zoomOut: function () {
        socialMap.map.zoomOut();
    },
    clearCompanyLayer: function () {
        if (socialMap.companyLayer) {
            socialMap.companyLayer.clearLayers();
        }
    },

    clearManualLayer: function () {
        if (socialMap.manualLayer) {
            socialMap.manualLayer.clearLayers();
        }
    },

    /**
     * Shows a route on the map. <b>NOTE the order of longitude, latitude </b>
     * @param lonLatString semicolon separated list of waypoints: 51,12;52.12,8.12[;...]
     */
     /*
    showRoute: function mainReturn(lonLatString) {
        $.get('https://api.mapbox.com/v4/directions/mapbox.driving/' + lonLatString + '.json?alternatives=false&access_token=' + L.mapbox.accessToken,
            function (route) {
                socialMap.map.addLayer(socialMap.map.featureLayer);
                socialMap.map.featureLayer.setGeoJSON(route.routes[0].geometry).setStyle({
                    color: '#08acf0',
                    weight: 5
                });
                socialMap.map.fitBounds(socialMap.map.featureLayer.getBounds());
            });
    },*/

    panMapToMarker: function (marker) {
        var currentZoom = socialMap.map.getZoom();
        socialMap.map.setView(marker.getLatLng(), currentZoom);
        var currentLatLng = $.extend({}, marker.getLatLng());
        currentLatLng.lng = currentLatLng.lng - 4 / Math.pow(2, (currentZoom - 7));
        socialMap.map.panTo(currentLatLng);
    },
/*
    addMarker: function (marker) {
        socialMap.markerCluster.addLayer(marker);
        socialMap.map.addLayer(socialMap.markerCluster);
    },

    removeMarker: function (marker) {
        socialMap.markerCluster.removeLayer(marker);
    },
*/
    addHeatmapLayer: function (geojson) {
        socialMap.map.addSource("events", {
            type: "geojson",
            data: geojson,
            cluster: true,
            clusterMaxZoom: 18, // Max zoom to cluster points on
            clusterRadius: 10 // Use small cluster radius for the heatmap look
        });

        var layers = [
            [0, 'rgba(39, 170, 225, 0.8)', 10],
            [10, 'rgba(43, 57, 144, 0.8)' , 12],
            [30, 'rgba(146, 39, 143, 0.8)', 14],
            [50, 'rgba(218, 28, 92, 0.8)', 18]
        ];

        layers.forEach(function (layer, i) {
            var radius = layer[2];
            socialMap.map.addLayer({
                "id": "cluster-" + i,
                "type": "circle",
                "source": "events",
                "paint": {
                    "circle-color": layer[1],
                    "circle-radius": {
                        'base': radius,
                        'stops': [[6, radius], [6.5, radius * 1.4], [7, radius * 1.96], [7.5, radius * 2.5], [8, radius * 2], [8.5, radius * 2], [9, radius * 2], [10, radius * 3], [12, radius * 2]]
                    },
                    "circle-blur": 0 // blur the circles to get a heatmap look
                },
                "filter": i === layers.length - 1 ?
                    [">=", "point_count", layer[0]] :
                    ["all",
                        [">=", "point_count", layer[0]],
                        ["<", "point_count", layers[i + 1][0]]]
            }, 'waterway-label');
        });

        var pointlayers = [
            [0, '#257da5', 2],
            [10, '#233187', 2],
            [30, '#82267e', 4],
            [50, '#b11a47', 4]
        ];

        pointlayers.forEach(function (layer, i) {
            socialMap.map.addLayer({
                "id": "cluster-point-" + i,
                "type": "circle",
                "source": "events",
                "paint": {
                    "circle-color": layer[1],
                    "circle-radius": layer[2],
                    "circle-blur": 0 // blur the circles to get a heatmap look
                },
                "filter": i === pointlayers.length - 1 ?
                    [">=", "point_count", layer[0]] :
                    ["all",
                        [">=", "point_count", layer[0]],
                        ["<", "point_count", pointlayers[i + 1][0]]]
            }, 'waterway-label');
        });
        socialMap.map.addLayer({
            "id": "unclustered-points",
            "type": "circle",
            "source": "events",
            "paint": {
                "circle-color": "rgba(100, 255, 0, 0.5)",
                "circle-radius": 0,
                "circle-blur": 1
            },
            "filter": ["!=", "cluster", true]
        }, 'waterway-label')
    }
};
