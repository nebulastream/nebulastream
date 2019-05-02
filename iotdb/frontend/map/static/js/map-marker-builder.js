var MapMarkerBuilder = {
    buildMarker: function (data) {
        let symbolIcon = null;
        if (data.feedSource === "Company") {
            symbolIcon = config.assetsPath + "/images/icons/other/icon_company-marker.svg";
        } else if (data.eventType) {
            symbolIcon = MapPopupBuilder.translateSymbolIcon(data.eventType[0]);
        }

        // don't continue if we cannot specify the source
        if (symbolIcon == null) {
            return;
        }

        let marker = L.marker(new L.LatLng(data.displayLocation.latitude, data.displayLocation.longitude));
        marker.symbolIcon = symbolIcon;
        marker.setIcon(this.buildIcon(symbolIcon, data.feedSource != "Company" ? config.smallMarkerSize : config.mediumMarkerSize));

        // renders a colored route for each trafficflow marker
        
        if (data.feedSource === "TrafficFlow" && data.segment != undefined) {
            $.get('https://api.mapbox.com/v4/directions/mapbox.driving/' + data.segment + '.json?alternatives=false&access_token=' + L.mapbox.accessToken,
                function (route) {
                    let routeLayer = L.geoJson(route.routes[0].geometry);

                    if (marker.isVisible) {
                        socialMap.trafficLayer.addLayer(routeLayer);
                    }

                    if (data.eventType[0] == "TrafficFlowSlowTraffic") {
                        routeLayer.setStyle({
                            color: 'yellow',
                            weight: 5,
                            opacity: 1
                        });
                    } else {
                        routeLayer.setStyle({
                            color: 'red',
                            weight: 5,
                            opacity: 1
                        });
                    }

                    marker.trafficLayer = routeLayer;
                });
        }
        

        marker.on('popupopen', function() {
            $(marker._icon).addClass("open");
            popup.show(marker);
        }).on('popupclose', function () {
            $(marker._icon).removeClass("open");
            popup.hide();
        });
        marker.on("click", function (e) {
            socialMap.panMapToMarker(marker);
        });
        return marker;
    },

    buildIcon: function(symbolIcon, size) {
        return L.icon({
            "iconUrl": symbolIcon,
            "iconSize": [size, size],
            "iconAnchor": [size/2, size/2],
            "popupAnchor": [0, -15],
            "className": "dot"
        })
    },

    buildCompanyMarker: function (data) {
        let marker = this.buildMarker(data);

        if (marker == null) return;

        marker.companyId = data.id;
        marker.companyName = data.companyName;
        marker.source = "Company";

        // TODO remove this: it is only a workaround to listen on a popup close event
        marker.bindPopup("", { className: "Company" });

        return marker;
    },

    buildEventMarker: function(event) {
        let marker = this.buildMarker(event);

        if (marker == null) return;

        marker.clusterId = event.clusterId;
        marker.date = event.indexDate;
        marker.source = event.feedSource;
        marker.sourceURL = event.sourceURL;
        marker.sourceDomain = event.feedSource === "TrafficFlow" ? "Q&V Hessen" : event.sourceDomain;
        marker.transportation = event.tweetType;
        marker.contentIndex = 0;

        if (event.eventType != undefined) {
            marker.eventType = event.eventType[0];
        }

        marker.isDetailed = false;

        let isTweet = event.eventSourceType === "TWITTER";
        let title = isTweet ? event.attributes.twitterUserID : event.title;

        marker.content = [{
            title: title,
            text: event.text,
            displayDate: event.indexDate,
            bratAnnotation: event.bratAnnotation,
            entityClasses: MapPopupBuilder.getEntityClassNames(event.bratAnnotation)
        }];

        marker.updateContent = this.updateContent;
        marker.changeContent = this.changeContent;
        marker.bindContentControlCallbacks = this.bindContentControlCallbacks;
        marker.addEventToMarker = this.addEventToMarker;
        marker.getText = this.getText;
        marker.getTitle = this.getTitle;
        marker.getDisplayDate = this.getDisplayDate;
        marker.getAnnotatedText = this.getAnnotatedText;
        marker.renderAnnotatedText = this.renderAnnotatedText;
        marker.toggleDetails = this.toggleDetails;
        marker.hasDetails = this.hasDetails;
        marker.isCompanyEvent = this.isCompanyEvent;
        marker.isTrafficEvent = this.isTrafficEvent;
        marker.getBroadEventType = this.getBroadEventType;
        marker.updateSourceIcons = this.updateSourceIcons;

        marker.data = {
            streetName: event.streetName,
            cityName: event.cityName,
            stopName: event.stopName
        };

        marker.bindPopup(MapPopupBuilder.buildFromMarker(marker), {
            offset:  new L.Point(-230, 30),
            maxWidth: 1000,
            maxHeight: 500,
            className: event.feedSource
        });
        marker.isPopupOpen = this.isPopupOpen;

        // Add a callback to reset the marker's content state before opening a popup
        marker.on("click", function () {
            this.contentIndex = 0;
            this.isDetailed = false;
            this.updateContent();
        });

        return marker;
    },

    addEventToMarker: function (event) {
        let isTweet = event.eventSourceType === "TWITTER";
        let title = isTweet ? event.attributes.twitterUserID : event.title;

        this.content.push({
            title: title,
            text: event.text,
            displayDate: event.indexDate,
            bratAnnotation: event.bratAnnotation,
            entityClasses: MapPopupBuilder.getEntityClassNames(event.bratAnnotation)
        });

        if (moment(event.indexDate) > moment(this.date))
            this.date = event.indexDate;

        if (this.isPopupOpen()) {
            this.updateContent();
        } else {
            let popupOffset = this._popup.options.offset;
            let contentLength = this.content.length;
            switch(true) {
                case contentLength >= 2 && contentLength <= 10:
                    this.setIcon(MapMarkerBuilder.buildIcon(this.symbolIcon, config.mediumMarkerSize));
                    break;
                case contentLength > 10:
                    this.setIcon(MapMarkerBuilder.buildIcon(this.symbolIcon, config.bigMarkerSize));
                    break;
                case contentLength <= 1:
                default:
                    this.setIcon(MapMarkerBuilder.buildIcon(this.symbolIcon, config.smallMarkerSize));
                    break;
            }
            // NOTE: This is a workaround, due to a bug in leaflet, which causes the popup to lose its offset
            this._popup.options.offset = popupOffset;
        }
    },

    isCompanyEvent: function () {
        return $.inArray(this.eventType, eventMappings.companyEvents) != -1;
    },

    isTrafficEvent: function () {
        return $.inArray(this.eventType, eventMappings.trafficEvents) != -1 ||
            $.inArray(this.eventType, eventMappings.trafficFlowEvents) != -1;
    },

    getBroadEventType: function () {
        return this.isCompanyEvent() ? "Company" : "Traffic";
    },

    isPopupOpen: function () {
        return this._popup._isOpen;
    },

    changeContent: function (changeIndex) {
        this.contentIndex += changeIndex;
        if (this.contentIndex < 0)
            this.contentIndex = 0;
        else if (this.contentIndex >= this.content.length)
            this.contentIndex = this.content.length - 1;

        this.updateContent();
    },

    renderAnnotatedText: function (bratAnnotation) {
        if (bratAnnotation == null) return;

        let parsedBratAnnotation = JSON.parse(bratAnnotation.substring("org.json.JSONObject:".length));
        return new TextAnnotator().annotate(parsedBratAnnotation);
    },

    getAnnotatedText: function () {
        return this.renderAnnotatedText(this.content[this.contentIndex].bratAnnotation);
    },

    getText: function () {
        return this.content[this.contentIndex].text;
    },

    getTitle: function () {
        return this.content[this.contentIndex].title;
    },

    getDisplayDate: function () {
        return this.content[this.contentIndex].displayDate;
    },

    toggleDetails: function () {
        this.isDetailed = !this.isDetailed;
        this.updateContent();
    },

    hasDetails: function () {
        return this.content[this.contentIndex].bratAnnotation != null ||
            this.content[this.text] > MapPopupBuilder.maxContentLength;
    },

    updateSourceIcons: function (content) {
        let activeSources = content.entityClasses;

        $('.sources-db li').hide();
        activeSources.forEach(function(activeSource) {
            $('.sources-db li.' + activeSource).show();
        });
    },

    updateContent: function () {
        this._popup.setContent(MapPopupBuilder.buildFromMarker(this));

        if (this.isDetailed)
            $('.sources-db').addClass("detail");
        else
            $('.sources-db').removeClass("detail");

        this.updateSourceIcons(this.content[this.contentIndex]);
        this.bindContentControlCallbacks();
    },

    bindContentControlCallbacks: function () {
        $(".infowindow .cycle-forward").click(function() { this.changeContent(1) }.bind(this));
        $(".infowindow .cycle-backward").click(function() { this.changeContent(-1) }.bind(this));
        $(".infowindow .popup-box-show-more a").click(function() { this.toggleDetails() }.bind(this));
    },
};
