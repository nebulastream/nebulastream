    'use strict';

    var feedController = {
        _trafficRouteEndPoint: config.base,
        _map: null,
        _markerCluster: null,
        /**
         * {'id1': [[lat1, lng1], [lat2, lng2], ....], 'id2': []}
         * reset to empty after each all
         */
        _routes: {},
        _consumedRoutes: [],
        _trafficStatus: {},
        _markers: {},

        convertFeedObject: function (receivedFeed, type) {
            console.log('receiveFeed: ', receivedFeed);
            if (type === 'routes') {
                this._routes = receivedFeed;
            } else if (type === 'markers') {
                this._trafficStatus = receivedFeed;
            }
        },

        handleTrafficStatus: function () {
			//console.log(this._trafficStatus)
            for (let key in this._trafficStatus) {
                let status = this._trafficStatus[key];
                if (! (key in this._markers)) {
                    if (key < 5000) {
						console.log('Set markers init');
                        this._markers[key] = L.marker(status, {
                            title: `route-${key}`,
                            icon: L.icon({
                    iconUrl: 'static/assets/images/icons/transport-icons/U-Bahn.svg',
                    iconSize: [20,20],
                }),
                    });
                    } else if (key < 12000) {
                        this._markers[key] = L.marker(status, {
                            title: `route-${key}`,
                            icon: L.icon({
                    iconUrl: 'static/assets/images/icons/transport-icons/S-Bahn-Logo.svg',
                    iconSize: [20,20],
                }),
                    });
                    } else {
                         this._markers[key] = L.marker(status, {
                            title: `route-${key}`,
                            icon: L.icon({
                    iconUrl: 'static/assets/images/icons/transport-icons/BUS-Logo-BVG.svg',
                    iconSize: [20,20],
                }),
                    });
                    }

                    this._markers[key].bindTooltip(`route-${key}`).openTooltip();
                } else {
                    this._markers[key].setLatLng(status);
                }

                this._markerCluster.addLayer(this._markers[key]);
            }
            this._markerCluster.refreshClusters();
        },

    /*
        handleTrafficRoutes: function () {
            for (let key in this._routes) {
                if (! this._consumedRoutes.includes(key)) {
                    L.polyline(this._routes[key]).addTo(this._map);
                    this._consumedRoutes.push(key);
                }
            }
            feedController.clearRoutes();
        },
    */

        init: function () {
			console.log("init feed")
            this._map = map.getMap();
            if (! this._map) {
                throw "map is undefined";
            }
            this._markerCluster = map.getMakerCluster();

            //this.initialDataLoad();


			/* Update every 50 msecs
            window.setInterval(function () {
                let bound = map.getMap().getBounds();
                let minLat = bound._southWest.lat;
                let minLng = bound._southWest.lng;
                let maxLat = bound._northEast.lat;
                let maxLng = bound._northEast.lng;

                $.ajax({
                    url: config.urls.trafficStatus(minLat, minLng, maxLat, maxLng),
                    success: function (data) {
                        feedController.convertFeedObject(data, 'markers');
                        feedController.handleTrafficStatus();
                    },
                    crossDomain: true,

                    headers: {
                        'Access-Control-Allow-Origin': '*'
                    },
                })
				}, 50);*/
        },

        initialDataLoad: function () {
            let bound = this._map.getBounds();
            let minLat = bound._southWest.lat;
            let minLng = bound._southWest.lng;
            let maxLat = bound._northEast.lat;
            let maxLng = bound._northEast.lng;

            $.ajax({
                url: config.urls.routes(minLat, minLng, maxLat, maxLng),
                success: function (data) {
                    feedController.convertFeedObject(data, 'routes');
                    //feedController.handleTrafficRoutes();
                },

                headers: {
                    'Access-Control-Allow-Origin': '*'
                },
                crossDomain: true,
            });
            $.ajax({
                url: config.urls.trafficStatus(minLat, minLng, maxLat, maxLng),
                success: function (data) {
                    feedController.convertFeedObject(data, 'markers');
                    feedController.handleTrafficStatus();
                },

                headers: {
                    'Access-Control-Allow-Origin': '*'
                },

                crossDomain: true,
            })
        },

        clearRoutes: function () {
            this._routes = {};
        },


    };
