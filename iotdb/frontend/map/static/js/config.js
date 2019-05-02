'use strict';

$(document).ready(function(){
    //connect to the socket server.
    var socket = io.connect('http://' + document.domain + ':' + location.port + '/rec');

	sideBar.init();
	map.init();
	feedController.init();

    // receive new traffic status
    socket.on('newstatus', function(msg) {
		feedController.convertFeedObject(msg, 'markers')
		feedController.handleTrafficStatus();
	});
	
	// receive routes
    //socket.on('routes', function(msg) {
	//	feedController.convertFeedObject(msg, 'routes')
	//	feedController.handleTrafficStatus();
	//});

});


var config = {
    initialView: [52.5, 13.35],
    // base: 'http://localhost:3000/routes',
};

config.urls = {
	//routes (blue lines in the map)
    routes: function (minLat, minLng, maxLat, maxLng, options) {
        return `http://localhost:3000/routes?minLat=${minLat}&minLng=${minLng}&maxLat=${maxLat}&maxLng=${maxLng}`;
    },
    //current marker position requested every 5 seconds
    trafficStatus: function (minLat, minLng, maxLat, maxLng, options) {
    	let url = `http://localhost:3000/trafficStatus?minLat=${minLat}&minLng=${minLng}&maxLat=${maxLat}&maxLng=${maxLng}`;
        console.log(url);
        return url;
    },
};
