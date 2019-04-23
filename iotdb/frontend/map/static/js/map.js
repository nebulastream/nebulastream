//'use strict';
/*
$(document).ready(function(){
    //connect to the socket server.
    var socket = io.connect('http://' + document.domain + ':' + location.port + '/test');
    var numbers_received = [];

    //receive details from server
    socket.on('newnumber', function(msg) {
        console.log("Received number" + msg.number);
        //maintain a list of ten numbers
        if (numbers_received.length >= 10){
            numbers_received.shift()
        }            
        numbers_received.push(msg.number);
        numbers_string = '';
        for (var i = 0; i < numbers_received.length; i++){
            numbers_string = numbers_string + '<p>' + numbers_received[i].toString() + '</p>';
        }
        $('#log').html(numbers_string);
		console.log(number_string);
    });

});
*/
var map = {
    _initialView: config.initialView,
    _map: null,
    _trafficRouteLayers: new L.layerGroup(),
    _manualLayer: new L.layerGroup(),
    _markers: new L.markerClusterGroup({
        maxClusterRadius: 10,
        //iconCreateFunction: function (cluster) {
        //    return new L.divIcon({
        //        iconSize: [30, 30],
                //className: 'markCluster',
                //html: "<div class='markerClusterLabel'>" + cluster.getChildCount() + "</div>"
        //    });
        //}
    }),

    init: function () {
		console.log("adasd")
        if (this._map != null) {
            this._map.remove();
            sideBar.resetFilterSettings();
        }

        sideBar.destroySlider();
        sideBar.hideLoadingSpinner();
        sideBar.enableCompanySearch();

        this._map = L.map('map-canvas', {
            zoomControl: false,
            center: this._initialView,
            zoom: 13,
            layers: [
                L.tileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                    'attribution': 'Map data © <a href="http://openstreetmap.org">OpenStreetMap</a> contributors'
                })
            ]
        });
        this._trafficRouteLayers.addTo(this._map);
        this._manualLayer.addTo(this._map);
        // this._markers.addTo(this._map);
        this._map.addLayer(this._markers);
        // let bound = this._map.getBounds();
        // console.log("bound: ", bound);
        // let marker1 = L.marker(bound._southWest);
        // let marker2 = L.marker(bound._northEast);
        // this._trafficRouteLayers.addLayer(marker1);
        // this._trafficRouteLayers.addLayer(marker2);
        // close the side menu when the map is clicked
        this._map.on("click", function() {
            $(".c-menu__close").click()
        })
    },
    getMap: function () {
        return this._map;
    },
    getMakerCluster: function () {
        return this._markers;
    },
    // getBounds: function() {
    //     return this._map.getBounds();
    // },



    /**
     * Centers the map and clears the manualLayer
     */
    resetView: function () {
        if (feedController.heatmap) {
            map._map.jumpTo({
                center: [52.5, 13.35],
                zoom: 13
            });
        } else {
            map._map.setView({
                lat: 52.5,
                lng: 13.35
            }, 13);
            //this.clearManualLayer();
        }

    },

    zoomIn: function () {
        map._map.zoomIn();
    },

    zoomOut: function () {
        map._map.zoomOut();
    },

};

// let latlngs = [
// ];

// $.ajax(
//     {
//         url: 'http://10.10.15.101:3000',
//         success: function (data) {
//             latlngs = data;
//         },
//         async: false,
//     }
// );

// const initialView = [52.001676, 12.776828]
// var map = new L.Map('map-canvas', {
//     center: initialView,
//     zoom: 11,
//     layers: [
//         new L.TileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
//             'attribution': 'Map data © <a href="http://openstreetmap.org">OpenStreetMap</a> contributors'
//         })
//     ]
// });


// const path = L.polyline(latlngs);

// let marker = L.marker(initialView).addTo(map);
// let i = 1;

// path.addTo(map);
// map.fitBounds(path.getBounds());

// window.setInterval(function () {
//     marker.setLatLng(latlngs[i]);
//     i += 1;
//     if (i >= latlngs.length) {
//         i = 0;
//     };
//     let bound = map.getBounds();
//     let marker1 = L.marker(bound._southWest).addTo(map);
//     let marker2 = L.marker(bound._northEast).addTo(map);

// }, 1000);


// console.log("bound: ", bound);

