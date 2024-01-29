#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>
#include <Exceptions/LocationProviderException.hpp>
#include <Mobility/ReconnectSchedulePredictors/PreCalculatedReconnectSchedulePredictor.hpp>
#include <Mobility/ReconnectSchedulePredictors/ReconnectSchedule.hpp>
#include <Util/Common.hpp>
#include <Identifiers.hpp>
#include <fstream>
std::optional<NES::Spatial::Mobility::Experimental::ReconnectSchedule>
NES::Spatial::Mobility::Experimental::PreCalculatedReconnectSchedulePredictor::getReconnectSchedule(
    const NES::Spatial::DataTypes::Experimental::Waypoint&,
    const NES::Spatial::DataTypes::Experimental::GeoLocation&,
    const S2PointIndex<uint64_t>&,
    bool) {
    return {};
}
NES::Spatial::Mobility::Experimental::PreCalculatedReconnectSchedulePredictor::PreCalculatedReconnectSchedulePredictor(
    const NES::Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& configuration)
    : ReconnectSchedulePredictor(configuration), startTime(getTimestamp()), csvPath(configuration->precalculatedReconnectPath) {
}
NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictorPtr NES::Spatial::Mobility::Experimental::PreCalculatedReconnectSchedulePredictor::create(
    const NES::Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& configuration) {
    return std::make_shared<PreCalculatedReconnectSchedulePredictor>(configuration);
}
void NES::Spatial::Mobility::Experimental::PreCalculatedReconnectSchedulePredictor::loadReconnectSimulationDataFromFile() {
    std::string csvLine;
    std::ifstream inputStream(csvPath);
    std::string parentIdString;
    std::string timeString;
    std::basic_string<char> delimiter = {','};
    NES_DEBUG("Started csv location source at {}", startTime)

    while (std::getline(inputStream, csvLine)) {
        std::stringstream stringStream(csvLine);
        std::vector<std::string> values;
        try {
            values = NES::Util::splitWithStringDelimiter<std::string>(csvLine, delimiter);
        } catch (std::exception& e) {
            std::string errorString =
                std::string("An error occurred while splitting delimiter of reconnect csv CSV. ERROR: ") + strerror(errno);
            NES_ERROR("Pre calculated reconnect predictor:  {}", errorString);
            //todo: proper exception here
            throw Spatial::Exception::LocationProviderException(errorString);
        }
        if (values.size() != 2) {
            std::string errorString =
                std::string("Pre calculated reconnect predictor: could not read reconnects from csv, expected 2 columns but input file has ")
                + std::to_string(values.size()) + std::string(" columns");
            NES_ERROR("Pre calculated reconnect predictor:  {}", errorString);
            throw Spatial::Exception::LocationProviderException(errorString);
        }
        parentIdString = values[0];
        timeString = values[1];

        Timestamp time;
        WorkerId parentId;

        try {
            time = std::stoul(timeString);
            parentId = std::stod(parentIdString);
        } catch (std::exception& e) {
            std::string errorString = std::string("An error occurred while creating the waypoint. ERROR: ") + strerror(errno);
            NES_ERROR("LocationProviderCSV: {}", errorString);
            throw Spatial::Exception::LocationProviderException(errorString);
        }
        NES_TRACE("Read from csv: {}, {}", parentId, time);

        //add startTime to the offset obtained from csv to get absolute timestamp
        time += startTime;

        reconnects.push_back({parentId, time});


    }
    if (reconnects.empty()) {
        std::string errorString = std::string("No data in CSV, cannot start location provider");
        NES_ERROR("LocationProviderCSV: {}", errorString);
        throw Spatial::Exception::LocationProviderException(errorString);
    }
    NES_DEBUG("read {} reconnects from csv", reconnects.size());
    NES_DEBUG("first timestamp is {}, last timestamp is {}",
              reconnects.front().second,
              reconnects.back().second);
    //set first csv entry as the next waypoint
}
NES::Spatial::Mobility::Experimental::PreCalculatedReconnectSchedulePredictor::~PreCalculatedReconnectSchedulePredictor() {}
