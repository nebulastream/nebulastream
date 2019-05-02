class EventValidation {

    static get ValidEventTypes() {
        return [
            //traffic
            "TrafficJam",
            "TrafficFlowTrafficJam",
            "TrafficFlowSlowTraffic",
            "Obstruction",
            "RailReplacementService",
            "Delay",
            "Accident",
            "Disaster",
            // company
            "Strike",
            "Layoffs",
            "Acquisition",
            "SpinOff",
            "Merger",
            "Insolvency"];
    }

    /**
     * @return {boolean} true if event contains all relevant properties
     */
    static validate(event) {
        let isOk = event &&
            event.eventType &&
            event.eventType.length &&
            EventValidation.ValidEventTypes.find(e => e === event.eventType[0]) &&
            event.geometry != null;
        if (!isOk && console.isDebug) {
            console.debug("event not valid", event);
        }
        return isOk;
    }
}
