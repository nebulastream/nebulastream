/**
 * Filters events by current filter settings
 *
 * @type {{initFilterSettings: eventFilter.initFilterSettings, resetFilterSettings: eventFilter.resetFilterSettings, toggleFilterSetting: eventFilter.toggleFilterSetting, getFilterSetting: eventFilter.getFilterSetting, isFiltered: eventFilter.isFiltered}}
 */
var eventFilter = {

    initFilterSettings: function () {
        this.company = false;
        this.traffic = false;

        this.rss = false;
        this.twitter = false;
        this.news = false;
        this.trafficFlow = false;

        this.street = false;
        this.rail = false;
        this.flight = false;
    },

    resetFilterSettings: function () {
        this.initFilterSettings();

        if (feedController && feedController.heatmap) {
            this.resetDisabledHeatmapFilters();
        }

        eventMarkersController.updateEventMarkers();
    },

    resetDisabledHeatmapFilters: function() {
        this.trafficFlow = true;
        this.street = true;
        this.rail = true;
        this.flight = true;
    },

    allSourcesEnabled: function() {
        return !this.rss && !this.twitter && !this.news;
    },

    enableAllSources: function() {
        this.rss = false;
        this.twitter = false;
        this.news = false;
    },

    disableAllSources: function() {
        this.rss = true;
        this.twitter = true;
        this.news = true;
    },

    toggleFilterSetting: function (filterSetting) {
        // Only allow one or all source filters to be active at the same time while in heatmap mode
        if (feedController && feedController.heatmap && ["rss", "twitter", "news"].includes(filterSetting)) {
            if (this.allSourcesEnabled())
            {
                this.disableAllSources();
                this[filterSetting] = false;
            }
            else if (this[filterSetting])
            {
                this.disableAllSources();
                this[filterSetting] = false;
            }
            else {
                this.enableAllSources();
            }
        } else {
            this[filterSetting] = !this[filterSetting];
        }

        if (feedController && feedController.heatmap) {
            this.resetDisabledHeatmapFilters();
        }

        eventMarkersController.updateEventMarkers();
    },

    getFilterSetting: function (filterSetting) {
        return this[filterSetting];
    },

    /**
     * ToDo: Rail, Flight, Street missing
     * @param marker an event marker
     * @returns {boolean} true if marker is filtered and should not be displayed, false if not filtered
     */
    isFiltered: function (marker) {
        if (marker.source === "Tweet" && this.twitter) {
            return true;
        } else if (marker.source === "Rss" && this.rss) {
            return true;
        } else if (marker.source === "News" && this.news) {
            return true;
        } else if (marker.source === "TrafficFlow" && this.trafficFlow) {
            return true;
        } else if (marker.isCompanyEvent() && this.company) {
            return true;
        } else if (marker.isTrafficEvent() && this.traffic) {
            return true;
        } else if (marker.transportation === "STRASSE" && this.street) {
            return true;
        } else if (marker.transportation === "SCHIENE" && this.rail) {
            return true;
        } else return marker.transportation === "LUFT" && this.flight;
    }

};
