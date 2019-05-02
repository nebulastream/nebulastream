/**
 * Provides translation and conversion for events from solr based on the eventType field.
 * iconMapping -> path to icon svg
 * simpleName -> name of the eventType
 * popupTitle -> transformer function for sidebar (mainly simpleName + location/ companyName) args: event object
 *
 * @type {{trafficEvents: [*], companyEvents: [*], getFirstAttributeName: eventMappings.getFirstAttributeName, TrafficJam: {iconMapping: string, popupTitle: eventMappings.TrafficJam.popupTitle, simpleName: string}, TrafficFlowTrafficJam: {iconMapping: string, popupTitle: eventMappings.TrafficFlowTrafficJam.popupTitle, simpleName: string}, TrafficFlowSlowTraffic: {iconMapping: string, popupTitle: eventMappings.TrafficFlowSlowTraffic.popupTitle, simpleName: string}, Obstruction: {iconMapping: string, popupTitle: eventMappings.Obstruction.popupTitle, simpleName: string}, RailReplacementService: {iconMapping: string, popupTitle: eventMappings.RailReplacementService.popupTitle, simpleName: string}, Delay: {iconMapping: string, popupTitle: eventMappings.Delay.popupTitle, simpleName: string}, Accident: {iconMapping: string, popupTitle: eventMappings.Accident.popupTitle, simpleName: string}, Disaster: {iconMapping: string, popupTitle: eventMappings.Disaster.popupTitle, simpleName: string}, companyPopupTitle: eventMappings.companyPopupTitle, Strike: {iconMapping: string, popupTitle: eventMappings.Strike.popupTitle, simpleName: string}, Layoffs: {iconMapping: string, popupTitle: eventMappings.Layoffs.popupTitle, simpleName: string}, Acquisition: {iconMapping: string, popupTitle: eventMappings.Acquisition.popupTitle, simpleName: string}, SpinOff: {iconMapping: string, popupTitle: eventMappings.SpinOff.popupTitle, simpleName: string}, Merger: {iconMapping: string, popupTitle: eventMappings.Merger.popupTitle, simpleName: string}, Insolvency: {iconMapping: string, popupTitle: eventMappings.Insolvency.popupTitle, simpleName: string}}}
 */
const eventMappings = {
    trafficEvents: ["TrafficJam", "Obstruction", "RailReplacementService", "Delay", "Accident", "Disaster"],
    companyEvents: ["Strike", "Layoffs", "Acquisition", "SpinOff", "Merger", "Insolvency"],
    trafficFlowEvents: ["TrafficFlowTrafficJam", "TrafficFlowSlowTraffic"],


    getFirstAttributeName: function(attribute) {
        return Array.isArray(attribute) ? attribute[0] : attribute;
    },

    "TrafficJam": {
        iconMapping: config.assetsPath + '/images/icons/traffic-events/icon_traffic-event_trafficjam.svg',
        popupTitle: function (data) {
            if (data.streetName != null) {
                return 'Traffic jam on ' + eventMappings.getFirstAttributeName(data.streetName);
            }
            if (data.cityName != null) {
                return 'Traffic jam in ' + eventMappings.getFirstAttributeName(data.cityName);
            }
            return this.simpleName;
        },
        simpleName: "Traffic jam"
    },
    "TrafficFlowTrafficJam": {
        iconMapping: config.assetsPath + '/images/icons/traffic-events/icon_traffic-event_trafficjam.svg',
        popupTitle: function (data) {
            return this.simpleName;
        },
        simpleName: "Traffic jam"
    },
    "TrafficFlowSlowTraffic": {
        iconMapping: config.assetsPath + '/images/icons/traffic-events/icon_traffic-event_camera.svg',
        popupTitle: function (data) {
            return this.simpleName;
        },
        simpleName: "Slow traffic"
    },
    "Obstruction": {
        iconMapping: config.assetsPath + '/images/icons/traffic-events/icon_traffic-event_roadblock.svg',
        popupTitle: function (data) {
            if (data.streetName != null) {
                return 'Obstructions on ' + eventMappings.getFirstAttributeName(data.streetName);
            }
            if (data.stopName != null) {
                return 'Obstructions at ' + eventMappings.getFirstAttributeName(data.stopName);
            }
            if (data.cityName != null) {
                return 'Obstructions in ' + eventMappings.getFirstAttributeName(data.cityName);
            }
            return this.simpleName;
        },
        simpleName: "Obstructions"
    },
    "RailReplacementService": {
        iconMapping: config.assetsPath + '/images/icons/traffic-events/icon_traffic-event_replacement.svg',
        popupTitle: function (data) {
            if (data.streetName != null) {
                return 'Rail replacement service on ' + eventMappings.getFirstAttributeName(data.streetName);
            }
            if (data.cityName != null) {
                return 'Rail replacement service in ' + eventMappings.getFirstAttributeName(data.cityName);
            }
            return this.simpleName;
        },
        simpleName: "Rail replacement service"
    },
    "Delay": {
        iconMapping: config.assetsPath + '/images/icons/traffic-events/icon_traffic-event_delay.svg',
        popupTitle: function (data) {
            if (data.stopName != null) {
                return 'Delay in ' + eventMappings.getFirstAttributeName(data.stopName);
            }
            return this.simpleName;
        },
        simpleName: "Delay"
    },
    "Accident": {
        iconMapping: config.assetsPath + '/images/icons/traffic-events/icon_traffic-event_accident.svg',
        popupTitle: function (data) {
            if (data.streetName != null) {
                return 'Accident on ' + eventMappings.getFirstAttributeName(data.streetName);
            }
            if (data.cityName != null) {
                return 'Accident in ' + eventMappings.getFirstAttributeName(data.cityName);
            }
            return this.simpleName;
        },
        simpleName: "Accident"
    },
    "Disaster": {
        iconMapping: config.assetsPath + '/images/icons/traffic-events/icon_traffic-event_weather.svg',
        popupTitle: function (data) {
            if (data.cityName != null) {
                return 'Disaster in ' + eventMappings.getFirstAttributeName(data.cityName);
            }
            return this.simpleName;
        },
        simpleName: "Disaster"
    },
    companyPopupTitle: function (data, type) {
        if (data.companyName != null) {
            return type + ' at ' + data.companyName;
        }
        return type;
    },
    "Strike": {
        iconMapping: config.assetsPath + '/images/icons/company-events/icon_company-event_strike.svg',
        popupTitle: function (data) {
            return eventMappings.companyPopupTitle(data, this.simpleName);
        },
        simpleName: "Strike"
    },
    "Layoffs": {
        iconMapping: config.assetsPath + '/images/icons/company-events/icon_company-event_dismissed.svg',
        popupTitle: function (data) {
            return eventMappings.companyPopupTitle(data, this.simpleName);
        },
        simpleName: "Layoffs"
    },
    "Acquisition": {
        iconMapping: config.assetsPath + '/images/icons/company-events/icon_company-event_acquisition.svg',
        popupTitle: function (data) {
            return eventMappings.companyPopupTitle(data, this.simpleName);
        },
        simpleName: "Acquisition"
    },
    "SpinOff": {
        iconMapping: config.assetsPath + '/images/icons/company-events/icon_company-event_spinoff.svg',
        popupTitle: function (data) {
            return eventMappings.companyPopupTitle(data, this.simpleName);
        },
        simpleName: "Spinoff"
    },
    "Merger": {
        iconMapping: config.assetsPath + '/images/icons/company-events/icon_company-event_merger.svg',
        popupTitle: function (data) {
            return eventMappings.companyPopupTitle(data, this.simpleName);
        },
        simpleName: "Merger"
    },
    "Insolvency": {
        iconMapping: config.assetsPath + '/images/icons/company-events/icon_company-event_insolvency.svg',
        popupTitle: function (data) {
            return eventMappings.companyPopupTitle(data, this.simpleName);
        },
        simpleName: "Insolvency"
    }
};

