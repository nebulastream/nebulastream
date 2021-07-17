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
#include <CoordinatorRPCService.pb.h>
#include <REST/runtime_utils.hpp>
#include <Util/Logger.hpp>

using namespace web;
using namespace http;

namespace NES {
StreamCatalogController::StreamCatalogController(StreamCatalogPtr streamCatalog) : streamCatalog(streamCatalog) {
    NES_DEBUG("StreamCatalogController()");
}

void StreamCatalogController::handleGet(std::vector<utility::string_t> path, web::http::http_request request) {

    //Extract parameters if any
    auto parameters = getParameters(request);

    if (path[1] == "allLogicalStream") {
        const std::map<std::string, std::string>& allLogicalStreamAsString = streamCatalog->getAllLogicalStreamAsString();

        auto param = parameters.find("physicalStreamName");

        json::value result{};
        if (allLogicalStreamAsString.empty()) {
            NES_DEBUG("No Logical Stream Found");
            resourceNotFoundImpl(request);
            return;
        } else if (param != parameters.end()) {
            try {
                // /streamCatalog/allLogicalStream?physicalStreamName=ICE2201
                std::string physicalStreamName = param->second;
                const std::map<std::string, std::string>& allLogicalStreamForPhysicalStream =
                    streamCatalog->getAllLogicalStreamForPhysicalStreamAsString(physicalStreamName);
                for (auto const& [key, val] : allLogicalStreamForPhysicalStream) {
                    result[key] = json::value::string(val);
                }
                successMessageImpl(request, result);
                return;
            } catch (const std::exception& exc) {
                NES_ERROR("StreamCatalogController: handleGet -allLogicalStreams (for a specific logical stream): Exception "
                          "occured while checking param for physicalStreamName"
                          << exc.what());
                handleException(request, exc);
                return;
            }
        } else {
            for (auto const& [key, val] : allLogicalStreamAsString) {
                result[key] = json::value::string(val);
            }
            successMessageImpl(request, result);
            return;
        }
    } else if (path[1] == "allPhysicalStream") {
        //Check if the path contains the query id
        auto param = parameters.find("logicalStreamName");
        if (param == parameters.end()) {
            NES_DEBUG("QueryController: No logicalStreamName was specified, returning all physical streams");
            json::value result{};
            const std::vector<StreamCatalogEntryPtr>& allPhysicalStream = streamCatalog->getPhysicalStreams();
            if (allPhysicalStream.empty()) {
                NES_DEBUG("No Physical Stream Found");
                resourceNotFoundImpl(request);
                return;
            } else {
                std::vector<json::value> allStream = {};
                for (auto const& physicalStream : std::as_const(allPhysicalStream)) {
                    allStream.push_back(json::value::string(physicalStream->toString()));
                }
                result["Physical Streams"] = json::value::array(allStream);
                successMessageImpl(request, result);
                return;
            }
        } else {
            // logical stream name specified
            try {
                //Prepare Input query from user string
                std::string logicalStreamName = param->second;

                const std::vector<StreamCatalogEntryPtr>& allPhysicalStream =
                    streamCatalog->getPhysicalStreams(logicalStreamName);

                //Prepare the response
                json::value result{};
                if (allPhysicalStream.empty()) {
                    NES_DEBUG("No Physical Stream Found");
                    resourceNotFoundImpl(request);
                    return;
                } else {
                    std::vector<json::value> allStream = {};
                    for (auto const& physicalStream : std::as_const(allPhysicalStream)) {
                        allStream.push_back(json::value::string(physicalStream->toString()));
                    }
                    result["Physical Streams"] = json::value::array(allStream);
                    successMessageImpl(request, result);
                    return;
                }
            } catch (const std::exception& exc) {
                NES_ERROR("StreamCatalogController: handleGet -allPhysicalStream: Exception occurred while building the "
                          "query plan for user request:"
                          << exc.what());
                handleException(request, exc);
                return;
            } catch (...) {
                RuntimeUtils::printStackTrace();
                internalServerErrorImpl(request);
                return;
            }
        }
    } else if (path[1] == "physicalStreamConfig") {
        auto param = parameters.find("streamName");
        if (param == parameters.end()) {
            NES_ERROR(
                "StreamCatalogController: handleGet -physicalStreamConfig: Exception occurred while tryig to retrieve config");
            json::value errorResponse{};
            errorResponse["detail"] = json::value::string("Parameter streamName must be provided");
            badRequestImpl(request, errorResponse);
            return;
        }
        else{

        }
    } else {
        resourceNotFoundImpl(request);
    }
}
void StreamCatalogController::handlePost(std::vector<utility::string_t> path, web::http::http_request message) {

    if (path[1] == "addLogicalStream") {
        NES_DEBUG("StreamCatalogController: handlePost -addLogicalStream: REST received request to add new Logical Stream "
                  << message.to_string());
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
                    NES_DEBUG("StreamCatalogController: handlePost -addLogicalStream: Try to add new Logical Stream "
                              << streamName << " and" << schema);
                    bool added = streamCatalog->addLogicalStream(streamName, schema);
                    NES_DEBUG("StreamCatalogController: handlePost -addLogicalStream: Successfully added new logical Stream ?"
                              << added);
                    //Prepare the response
                    json::value result{};
                    result["Success"] = json::value::boolean(added);
                    successMessageImpl(message, result);
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR("StreamCatalogController: handlePost -addLogicalStream: Exception occurred while trying to add new "
                              "logical stream"
                              << exc.what());
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
        NES_DEBUG("StreamCatalogController: handlePost -updateLogicalStream: REST received request to update Logical Stream "
                  << message.to_string());
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
                        NES_DEBUG("StreamCatalogController: handlePost -updateLogicalStream: unable to find stream "
                                  + streamName);
                        throw std::invalid_argument("Unable to update logical stream " + streamName);
                    }
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR("StreamCatalogController: handlePost -updateLogicalStream: Exception occurred while updating "
                              "Logical Stream."
                              << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                    return;
                }
            })
            .wait();
    } else if (path[1] == "addPhysicalToLogicalStream") {
        NES_DEBUG("StreamCatalogController: handlePost -addPhysicalToLogicalStream: REST received request to add a Physical "
                  "Stream to a Logical Stream "
                  << message.to_string());
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    std::shared_ptr<RegisterPhysicalStreamRequest> protobufMessage =
                        std::make_shared<RegisterPhysicalStreamRequest>();

                    if (!protobufMessage->ParseFromArray(body.data(), body.size())) {
                        json::value errorResponse{};
                        errorResponse["detail"] = json::value::string("Invalid Protobuf message");
                        badRequestImpl(message, errorResponse);
                        return;
                    }
                    NES_DEBUG("StreamCatalogController: handlePost -addPhysicalToLogicalStream: Start trying to add new physical "
                              "stream");
                    std::string physicalStreamName = protobufMessage->physicalstreamname();
                    std::string logicalStreamName = protobufMessage->logicalstreamname();
                    bool added;
                    if (logicalStreamName == "") {
                        NES_ERROR("StreamCatalogController: handlePost -addPhysicalToLogicalStream: Parameter logicalStreamName "
                                  "was not given.");
                        return;
                    } else {
                        NES_DEBUG("StreamCatalogController: handlePost -addPhysicalToLogicalStream: Try to add  Physical Stream "
                                  "to a Logical Stream"
                                  << logicalStreamName);
                        added = streamCatalog->addPhysicalStreamToLogicalStream(physicalStreamName, logicalStreamName);
                    }
                    NES_DEBUG("StreamCatalogController: handlePost -addPhysicalToLogicalStream: Successfully added  Physical "
                              "Stream to logical Stream?"
                              << added);
                    //Prepare the response
                    json::value result{};
                    result["Success"] = json::value::boolean(added);
                    successMessageImpl(message, result);
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR("StreamCatalogController: handlePost -addPhysicalToLogicalStream: Exception occurred while trying "
                              "to add  "
                              "physical stream to logical stream"
                              << exc.what());
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

void StreamCatalogController::handleDelete(std::vector<utility::string_t> path, web::http::http_request request) {

    //Extract parameters if any
    auto parameters = getParameters(request);

    if (path[1] == "deleteLogicalStream") {
        //Check if the path contains the query id
        auto param = parameters.find("streamName");
        if (param == parameters.end()) {
            NES_ERROR("StreamCatalogController: handleDelete -deleteLogicalStream: Exception occurred while trying to remove "
                      "logical stream");
            json::value errorResponse{};
            errorResponse["detail"] = json::value::string("Parameter queryId must be provided");
            badRequestImpl(request, errorResponse);
        }

        try {
            //Prepare Input query from user string
            std::string streamName = param->second;

            bool added = streamCatalog->removeLogicalStream(streamName);

            //Prepare the response
            json::value result{};
            if (added) {
                result["Success"] = json::value::boolean(added);
                successMessageImpl(request, result);
            } else {
                throw std::invalid_argument("Could not remove logical stream " + streamName);
            }

            return;
        } catch (const std::exception& exc) {
            NES_ERROR("StreamCatalogController: handleDelete -deleteLogicalStream: Exception occurred while building the "
                      "query plan for user request."
                      << exc.what());
            handleException(request, exc);
            return;
        } catch (...) {
            RuntimeUtils::printStackTrace();
            internalServerErrorImpl(request);
            return;
        }
    } else if (path[1] == "removePhysicalStreamFromLogicalStream") {
        auto logicalStreamName = parameters.find("logicalStreamName");
        if (logicalStreamName == parameters.end()) {
            NES_ERROR("StreamCatalogController: Unable to find parameter logicalStreamName");
            json::value errorResponse{};
            errorResponse["detail"] = json::value::string("Parameter logicalStreamName must be provided");
            badRequestImpl(request, errorResponse);
        }
        auto physicalStreamName = parameters.find("physicalStreamName");
        if (physicalStreamName == parameters.end()) {
            NES_ERROR("StreamCatalogController: Unable to find parameter physicalStreamName");
            json::value errorResponse{};
            errorResponse["detail"] = json::value::string("Parameter physicalStreamName must be provided");
            badRequestImpl(request, errorResponse);
        }

        try {
            std::string streamName = logicalStreamName->second;
            std::string phyStreamName = physicalStreamName->second;

            bool removed = streamCatalog->removePhysicalStreamFromLogicalStream(phyStreamName, streamName);

            //Prepare the response
            json::value result{};
            if (removed) {
                result["Success"] = json::value::boolean(removed);
                successMessageImpl(request, result);
            } else {
                throw std::invalid_argument("Could not remove physical stream " + phyStreamName + " from logical stream "
                                            + streamName);
            }

            return;
        } catch (const std::exception& exc) {
            NES_ERROR("StreamCatalogController: handleDelete -deleteLogicalStream: Exception occurred removing the "
                      "physical stream from the specified logical stream "
                      << exc.what());
            handleException(request, exc);
            return;
        } catch (...) {
            RuntimeUtils::printStackTrace();
            internalServerErrorImpl(request);
            return;
        }
    } else {
        resourceNotFoundImpl(request);
    }
}

}// namespace NES
