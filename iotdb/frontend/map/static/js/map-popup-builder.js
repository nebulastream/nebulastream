/**
 * Provides a convenient way to get the markup for the map tooltips.
 * Usage: MapPopupBuilder.buildFromMarker(eventMarker)
 *
 * @type {{popupTemplate: *, buildFromMarker: MapPopupBuilder.buildFromMarker, renderShowMoreLink: MapPopupBuilder.renderShowMoreLink, translateSourceClass: MapPopupBuilder.translateSourceClass, translateSymbolIcon: MapPopupBuilder.translateSymbolIcon}}
 */
let MapPopupBuilder = {
    popupTemplate: Handlebars.compile($("#map-popup-template").html()),
    maxContentLength: 140,

    buildFromMarker: function (marker) {
        var textContent = null;

        if (marker.isDetailed) {
            textContent = marker.getAnnotatedText() || marker.getText();
        } else {
            textContent = marker.getText();
            textContent = textContent.substring(0, MapPopupBuilder.maxContentLength) + (textContent.length > MapPopupBuilder.maxContentLength ? "..." : "")
        }

        let showMoreLink = marker.hasDetails() ? MapPopupBuilder.renderShowMoreLink(marker.isDetailed) : '';

        let popupType = "popup-box-content-" + (marker.isDetailed ? "long" : "short");

        let context = {
            infoWindowType: MapPopupBuilder.translateSourceClass(marker),
            encodedIconURI: encodeURI(MapPopupBuilder.translateSymbolIcon(marker.eventType)),

            title: marker.getTitle().substring(0, 50) + (marker.getTitle().length > 50 ? "..." : ""),
            sourceDomain: marker.sourceDomain || "twitter.com",
            formattedDate: moment(marker.getDisplayDate()).locale('de').tz("Europe/Berlin").format("dddd, DD.MM, HH:mm"),
            content: textContent,
            popupType: popupType,

            showMoreLink: showMoreLink,
            contentIndex: marker.contentIndex + 1,
            contentLength: marker.content.length,
            hasMultipleContent: marker.content.length > 1
        };
        return this.popupTemplate(context);
    },

    renderShowMoreLink: function (isOpened) {
        let linkLabel = (isOpened ? "hide" : "show") + " text analytics";
        return ('<div class="popup-box-show-more"><a href="javascript:">' + linkLabel + '</a></div>');
    },

    getEntityClassName: function (entityType) {
        if (entityType.match(/db\/route\/.*/i)) {
            return "entity-db-routes";
        } else if (entityType.match(/db\/stop\/.*/i)) {
            return "entity-db-stops";
        } else if (entityType.match(/date\/.*/i)) {
            return "entity-time";
        } else if (entityType.match(/duration\/.*/i)) {
            return "entity-time";
        } else {
            return "entity-" + entityType.split('/')[0];
        }
    },

    getEntityClassNames: function (bratAnnotationJson) {
        if (bratAnnotationJson == null)
            return [];

        let bratAnnotation = JSON.parse(bratAnnotationJson.substring("org.json.JSONObject:".length));

        let entities = bratAnnotation.result.entityLinking.entities;
        return entities.map(function (entity) {
            return MapPopupBuilder.getEntityClassName(entity[1]);
        }).filter((element, index, array) => array.indexOf(element) === index);
    },

    /**
     * Simple transformer to get the source icon for a tooltip (top left corner)
     */
    translateSourceClass: function (marker) {
        switch (marker.source) {
            case "News":
                return "news";
            case "Tweet":
                return "twitter";
            case 'bahn.de':
                return 'train';
            case 'TrafficFlow':
            case 'viz-info.de':
            case 'traffiq.de':
                return 'traffic';
            case 'polizei-bw.de':
            case 'polizei.sachsen.de':
            case 'polizei.bayern.de':
                return 'police';
        }
        return 'news';
    },

    /**
     * @returns {string} svg-icon for the given eventType or twitter icon
     */
    translateSymbolIcon: function (eventType) {
        return eventMappings[eventType] ?
            eventMappings[eventType].iconMapping :
            null;
    }
};
