/**
 * Handles the list of event markers and their visibility on the map
 *
 * @type {{eventMarkers: Array, trafficFlowMarkers: Array, markerCount: 
 {Tweet: number, News: number, Rss: number}, 
 addEvent: eventMarkersController.addEvent, 
 createEventMarker: eventMarkersController.createEventMarker, 
 addTrafficFlowEvents: eventMarkersController.addTrafficFlowEvents, 
 clearTrafficFlowEvents: eventMarkersController.clearTrafficFlowEvents, 
 getMarkerByClusterId: eventMarkersController.getMarkerByClusterId, 
 showMarker: eventMarkersController.showMarker, 
 hideMarker: eventMarkersController.hideMarker, 
 removeOutdatedMarkers: 
 eventMarkersController.removeOutdatedMarkers, 
 updateEventMarkers: 
 eventMarkersController.updateEventMarkers, 
 updateEventMarker: 
 eventMarkersController.updateEventMarker, 
 isMarkerOutdated: 
 eventMarkersController.isMarkerOutdated, 
 removeAllMarkers: 
 eventMarkersController.removeAllMarkers}}
 */
const eventMarkersController = {
    eventMarkers: [],
    trafficFlowMarkers: [],
    markerCount: {
        Tweet: 0,
        News: 0,
        Rss: 0,
        Company: 0,
        Traffic: 0
    },

    addEvent: function(event) {
        let marker = this.getMarkerByClusterId(event.clusterId);

        if (marker == null) {
            this.createEventMarker(event);
        } else {
            marker.addEventToMarker(event);
        }
    },

    updateMarkerCount: function (marker, diff) {
        this.markerCount[marker.source] += diff;
        this.markerCount[marker.getBroadEventType()] += diff;
        if (this.markerCount[marker.source] < 0 || this.markerCount[marker.getBroadEventType()] < 0)
            console.warn("Marker count invalid!");
    },

    createEventMarker: function(event) {
        let marker = MapMarkerBuilder.buildEventMarker(event);

        if (marker == null) {
            console.debug("Skipped adding invalid marker");
            return;
        }

        if (this.isMarkerOutdated(moment(), marker)) {
            console.debug("Skipped adding outdated marker");
            return;
        }

        this.eventMarkers.push(marker);
        this.updateMarkerCount(marker, 1);

        if (!eventFilter.isFiltered(marker) && !sideBar.isVisible() && !popup.isVisible())
            this.showMarker(marker);
        else
            this.hideMarker(marker);
    },

    addTrafficFlowEvents: function (events) {
        eventMarkersController.clearTrafficFlowEvents();
        events.forEach(function (event) {
            let marker = MapMarkerBuilder.buildEventMarker(event);

            if (marker == null) {
                console.debug("Skipped adding invalid marker");
                return;
            }

            this.trafficFlowMarkers.push(marker);

            if (!eventFilter.isFiltered(marker))
                this.showMarker(marker);
            else
                this.hideMarker(marker);
        }.bind(this));
    },

    clearTrafficFlowEvents: function () {
        eventMarkersController.trafficFlowMarkers.forEach(this.hideMarker.bind(this));
        eventMarkersController.trafficFlowMarkers = [];
    },

    getMarkerByClusterId: function(clusterId) {
        if (clusterId == null)
            return;

        return this.eventMarkers.find(function (marker) {
            return marker.clusterId === clusterId;
        });
    },

    showMarker: function (marker) {
        socialMap.addMarker(marker);
        if(marker.trafficLayer != null){
            socialMap.trafficLayer.addLayer(marker.trafficLayer);
        }
        marker.isVisible = true;
    },

    hideMarker: function (marker) {
        socialMap.removeMarker(marker);
        if(marker.trafficLayer!= null){
            socialMap.trafficLayer.removeLayer(marker.trafficLayer);
        }
        marker.isVisible = false;
    },

    /**
     * Remove outdated markers unless the marker's popup is currently opened
     */
    removeOutdatedMarkers: function () {
        let dateNow = moment();
        this.eventMarkers = this.eventMarkers.filter(function (marker) {
            if (this.isMarkerOutdated(dateNow, marker) && !marker.isPopupOpen()) {
                this.hideMarker(marker);
                this.updateMarkerCount(marker, -1);
                return false;
            } else {
                return true
            }
        }.bind(this));
    },

    updateEventMarkers: function () {
        this.removeOutdatedMarkers();
        this.eventMarkers.forEach(this.updateEventMarker.bind(this));
        this.trafficFlowMarkers.forEach(this.updateEventMarker.bind(this));
    },

    sortEventMarkersByDate: function() {
        this.eventMarkers = this.eventMarkers.sort(function(eventA, eventB) {
            let eventADate = moment(eventA.date);
            let eventBDate = moment(eventB.date);

            if (eventADate > eventBDate)
                return 1;
            else if (eventADate < eventBDate)
                return -1;
            else
                return 0;
        });
    },

    updateEventMarker: function(marker) {
        if (marker.isVisible && eventFilter.isFiltered(marker)) {
            this.hideMarker(marker);
        } else if (!marker.isVisible && !eventFilter.isFiltered(marker)) {
            this.showMarker(marker);
        }
    },

    /**
     * Indicates whether an eventMarker is outdated
     * @param dateNow the current date
     * @param marker the eventMarker to be checked
     * @returns {boolean} true: eventMarker is outdated, false: eventMarker is still valid
     */
    isMarkerOutdated: function(dateNow, marker) {
        if (this.markerCount[marker.source] <= config.minEventsPerSource)
            return false;

        if (this.markerCount[marker.getBroadEventType()] <= config.minEventsPerType)
            return false;

        let markerDate = moment(marker.date);
        let markerAge = moment.duration(dateNow - markerDate);
        return markerAge > config.validityPeriod;
    },

    removeAllMarkers: function () {
        eventMarkersController.eventMarkers.forEach(function (marker) {
            eventMarkersController.hideMarker(marker);
        });
        eventMarkersController.trafficFlowMarkers.forEach(function (marker) {
            eventMarkersController.hideMarker(marker);
        });
        socialMap.map.eachLayer(function(layer){
            socialMap.map.removeLayer(layer);
        });

        eventMarkersController.markerCount = {
            Tweet: 0,
            News: 0,
            Rss: 0,
            Company: 0,
            Traffic: 0
        };
        eventMarkersController.eventMarkers = [];
        eventMarkersController.trafficFlowMarkers = [];
    }
};
