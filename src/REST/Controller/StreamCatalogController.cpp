/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include "REST/Controller/StreamCatalogController.hpp"
#include <Catalogs/StreamCatalog.hpp>
#include <REST/runtime_utils.hpp>
#include <Util/Logger.hpp>

using namespace web;
using namespace http;

namespace NES {
StreamCatalogController::StreamCatalogController(StreamCatalogPtr streamCatalog) : streamCatalog(streamCatalog) {
    NES_DEBUG("StreamCatalogController()");
}

void StreamCatalogController::handleGet(std::vector<utility::string_t> path, web::http::http_request message) {

    if (path[1] == "allLogicalStream") {
        const std::map<std::string, std::string>& allLogicalStreamAsString = streamCatalog->getAllLogicalStreamAsString();

        json::value result{};
        if (allLogicalStreamAsString.empty()) {
            NES_DEBUG("No Logical Stream Found");
            resourceNotFoundImpl(message);
            return;
        } else {
            for (auto const& [key, val] : allLogicalStreamAsString) {
                result[key] = json::value::string(val);
            }
            successMessageImpl(message, result);
            return;
        }
    } else if (path[1] == "allPhysicalStream") {
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    //Prepare Input query from user string
                    std::string payload(body.begin(), body.end());

                    json::value req = json::value::parse(payload);

                    std::string logicalStreamName = req.at("streamName").as_string();

                    const std::vector<StreamCatalogEntryPtr>& allPhysicalStream = streamCatalog->getPhysicalStreams(logicalStreamName);

                    //Prepare the response
                    json::value result{};
                    if (allPhysicalStream.empty()) {
                        NES_DEBUG("No Physical Stream Found");
                        resourceNotFoundImpl(message);
                        return;
                    } else {
                        std::vector<json::value> allStream = {};
                        for (auto const& physicalStream : std::as_const(allPhysicalStream)) {
                            allStream.push_back(json::value::string(physicalStream->toString()));
                        }
                        result["Physical Streams"] = json::value::array(allStream);
                        successMessageImpl(message, result);
                        return;
                    }
                } catch (const std::exception& exc) {
                    NES_ERROR("StreamCatalogController: handleGet -allPhysicalStream: Exception occurred while building the query plan for user request:" << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                    return;
                }
                return;
            })
            .wait();
    } else {
        resourceNotFoundImpl(message);
    }
}

void StreamCatalogController::handlePost(std::vector<utility::string_t> path, web::http::http_request message) {

    if (path[1] == "addLogicalStream") {

        NES_DEBUG("StreamCatalogController: handlePost -addLogicalStream: REST received request to add new Logical Stream " << message.to_string());
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    NES_DEBUG("StreamCatalogController: handlePost -addLogicalStream: Start trying to add new logical stream");
                    //Prepare Input query from user string
                    std::string payload(body.begin(), body.end());
                    NES_DEBUG("StreamCatalogController: handlePost -addLogicalStream: userRequest: " << payload);
                    json::value req = json::value::parse(payload);
                    NES_DEBUG("StreamCatalogController: handlePost -addLogicalStream: Json Parse Value: " << req);
                    std::string streamName = req.at("streamName").as_string();
                    std::string schema = req.at("schema").as_string();
                    NES_DEBUG("StreamCatalogController: handlePost -addLogicalStream: Try to add new Logical Stream " << streamName << " and" << schema);
                    bool added = streamCatalog->addLogicalStream(streamName, schema);
                    NES_DEBUG("StreamCatalogController: handlePost -addLogicalStream: Successfully added new logical Stream ?" << added);
                    //Prepare the response
                    json::value result{};
                    result["Success"] = json::value::boolean(added);
                    successMessageImpl(message, result);
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR("StreamCatalogController: handlePost -addLogicalStream: Exception occurred while trying to add new logical stream" << exc.what());
                    handleException(message, exc);
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
                    std::string userRequest(body.begin(), body.end());
                    NES_DEBUG("StreamCatalogController: handlePost -updateLogicalStream: userRequest: " << userRequest);
                    json::value req = json::value::parse(userRequest);

                    std::string streamName = req.at("streamName").as_string();
                    std::string schema = req.at("schema").as_string();

                    bool updated = streamCatalog->updatedLogicalStream(streamName, schema);

                    if (updated) {
                        //Prepare the response
                        json::value result{};
                        result["Success"] = json::value::boolean(updated);
                        successMessageImpl(message, result);
                    } else {
                        NES_DEBUG("StreamCatalogController: handlePost -updateLogicalStream: unable to find stream " + streamName);
                        throw std::invalid_argument("Unable to update logical stream " + streamName);
                    }
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR("StreamCatalogController: handlePost -updateLogicalStream: Exception occurred while updating Logical Stream." << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                    return;
                }
            })
            .wait();
    } else {
        resourceNotFoundImpl(message);
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

                    std::string streamName = req.at("streamName").as_string();

                    bool added = streamCatalog->removeLogicalStream(streamName);

                    //Prepare the response
                    json::value result{};
                    if (added) {
                        result["Success"] = json::value::boolean(added);
                        successMessageImpl(message, result);
                    } else {
                        throw std::invalid_argument("Could not remove logical stream " + streamName);
                    }

                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR("StreamCatalogController: handleDelete -deleteLogicalStream: Exception occurred while building the query plan for user request." << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                    return;
                }
            })
            .wait();
    } else {
        resourceNotFoundImpl(message);
    }
}

}// namespace NES
