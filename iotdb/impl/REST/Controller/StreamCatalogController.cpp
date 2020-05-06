#include "REST/Controller/StreamCatalogController.hpp"
#include <REST/runtime_utils.hpp>
#include <Util/Logger.hpp>

using namespace web;
using namespace http;

namespace NES {

void StreamCatalogController::handleGet(std::vector<utility::string_t> path, web::http::http_request message) {

    if (path[1] == "allLogicalStream") {
        const map<std::string, std::string>& allLogicalStreamAsString = streamCatalogServicePtr->getAllLogicalStreamAsString();

        json::value result{};
        if (allLogicalStreamAsString.empty()) {
            result = json::value::string("No Logical Stream Found.");
        } else {
            for (auto const& [key, val] : allLogicalStreamAsString) {
                result[key] = json::value::string(val);
            }
        }
        successMessageImpl(message, result);
        return;

    } else if (path[1] == "allPhysicalStream") {
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    //Prepare Input query from user string
                    std::string payload(body.begin(), body.end());

                    json::value req = json::value::parse(payload);

                    string logicalStreamName = req.at("streamName").as_string();

                    const vector<StreamCatalogEntryPtr>& allPhysicalStream = streamCatalogServicePtr->getAllPhysicalStream(logicalStreamName);

                    //Prepare the response
                    json::value result{};
                    if (allPhysicalStream.empty()) {
                        result = json::value::string("No Physical Stream Found.");
                    } else {
                        vector<json::value> allStream = {};
                        for (auto const& physicalStream : std::as_const(allPhysicalStream)) {
                            allStream.push_back(json::value::string(physicalStream->toString()));
                        }
                        result["Physical Streams"] = json::value::array(allStream);
                    }
                    successMessageImpl(message, result);
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR("StreamCatalogController: handleGet -allPhysicalStream: Exception occurred while building the query plan for user request:" << exc.what());
                    internalServerErrorImpl(message);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                    return;
                }
            })
            .wait();
    }

    resourceNotFoundImpl(message);
}

void StreamCatalogController::handlePost(std::vector<utility::string_t> path, web::http::http_request message) {

    try {

        if (path[1] == "addLogicalStream") {

            NES_DEBUG("StreamCatalogController: handlePost -addLogicalStream: REST received request to add new Logical Stream " << message.to_string());
            message.extract_string(true)
                .then([this, message](utility::string_t body) {
                    try {
                        NES_DEBUG("StreamCatalogController: handlePost -addLogicalStream: Start trying to add new logical stream");
                        //Prepare Input query from user string
                        string payload(body.begin(), body.end());
                        NES_DEBUG("StreamCatalogController: handlePost -addLogicalStream: userRequest: " << payload);
                        json::value req = json::value::parse(payload);
                        NES_DEBUG("StreamCatalogController: handlePost -addLogicalStream: Json Parse Value: " << req);
                        string streamName = req.at("streamName").as_string();
                        string schema = req.at("schema").as_string();
                        NES_DEBUG("StreamCatalogController: handlePost -addLogicalStream: Try to add new Logical Stream " << streamName << " and" << schema);
                        bool added = streamCatalogServicePtr->addNewLogicalStream(streamName, schema);
                        NES_DEBUG("StreamCatalogController: handlePost -addLogicalStream: Successfully added new logical Stream ?" << added);
                        //Prepare the response
                        json::value result{};
                        result["Success"] = json::value::boolean(added);
                        successMessageImpl(message, result);
                        return;
                    } catch (const std::exception& exc) {
                        NES_ERROR("StreamCatalogController: handlePost -addLogicalStream: Exception occurred while trying to add new logical stream" << exc.what());
                        internalServerErrorImpl(message);
                        return;
                    } catch (...) {
                        RuntimeUtils::printStackTrace();
                        internalServerErrorImpl(message);
                        return;
                    }
                })
                .wait();
        } else if (path[1] == "updateLogicalStream") {
            NES_DEBUG("StreamCatalogController: handlePost -updateLogicalStream: REST received request to update Logical Stream " << message.to_string());
            message.extract_string(true)
                .then([this, message](utility::string_t body) {
                    try {
                        NES_DEBUG("StreamCatalogController: handlePost -updateLogicalStream: Start trying to update logical stream");
                        //Prepare Input query from user string
                        string userRequest(body.begin(), body.end());
                        NES_DEBUG("StreamCatalogController: handlePost -updateLogicalStream: userRequest: " << userRequest);
                        json::value req = json::value::parse(userRequest);

                        string streamName = req.at("streamName").as_string();
                        string schema = req.at("schema").as_string();

                        bool added = streamCatalogServicePtr->updatedLogicalStream(streamName, schema);

                        //Prepare the response
                        json::value result{};
                        result["Success"] = json::value::boolean(added);
                        successMessageImpl(message, result);

                        return;
                    } catch (const std::exception& exc) {
                        NES_ERROR("StreamCatalogController: handlePost -updateLogicalStream: Exception occurred while updating Logical Stream." << exc.what());
                        internalServerErrorImpl(message);
                        return;
                    } catch (...) {
                        RuntimeUtils::printStackTrace();
                        internalServerErrorImpl(message);
                        return;
                    }
                })
                .wait();
        }

        resourceNotFoundImpl(message);
    } catch (const std::exception& ex) {
        NES_ERROR("StreamCatalogController: handlePost: Exception occurred during post request." << ex.what());
        internalServerErrorImpl(message);
    } catch (...) {
        RuntimeUtils::printStackTrace();
        internalServerErrorImpl(message);
    }
}

void StreamCatalogController::handleDelete(std::vector<utility::string_t> path, web::http::http_request message) {

    if (path[1] == "deleteLogicalStream") {
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    //Prepare Input query from user string
                    std::string payload(body.begin(), body.end());

                    json::value req = json::value::parse(payload);

                    string streamName = req.at("streamName").as_string();

                    bool added = streamCatalogServicePtr->removeLogicalStream(streamName);

                    //Prepare the response
                    json::value result{};
                    result["Success"] = json::value::boolean(added);
                    successMessageImpl(message, result);

                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR("StreamCatalogController: handleDelete -deleteLogicalStream: Exception occurred while building the query plan for user request." << exc.what());
                    internalServerErrorImpl(message);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                    return;
                }
            })
            .wait();
    }
    resourceNotFoundImpl(message);
}

}// namespace NES
