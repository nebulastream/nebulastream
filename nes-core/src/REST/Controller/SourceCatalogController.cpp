/*
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

#include <Catalogs/Source/SourceCatalog.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <REST/Controller/SourceCatalogController.hpp>
#include <SerializableOperator.pb.h>
#include <Util/Logger/Logger.hpp>
#include <cpprest/details/basic_types.h>
#include <cpprest/http_msg.h>
#include <utility>

namespace NES {
SourceCatalogController::SourceCatalogController(Catalogs::Source::SourceCatalogPtr sourceCatalog)
    : sourceCatalog(std::move(sourceCatalog)) {
    NES_DEBUG("SourceCatalogController()");
}

void SourceCatalogController::handleGet(const std::vector<utility::string_t>& path, web::http::http_request& request) {

    //Extract parameters if any
    auto parameters = getParameters(request);

    if (path[1] == "allLogicalSource") {
        const std::map<std::string, std::string>& allLogicalSourceAsString = sourceCatalog->getAllLogicalSourceAsString();

        web::json::value result{};
        if (allLogicalSourceAsString.empty()) {
            NES_DEBUG("No Logical Source Found");
            resourceNotFoundImpl(request);
            return;
        }
        for (auto const& [key, val] : allLogicalSourceAsString) {
            result[key] = web::json::value::string(val);
        }
        successMessageImpl(request, result);
        return;

    } else if (path[1] == "allPhysicalSource") {
        //Check if the path contains the query id
        auto param = parameters.find("logicalSourceName");
        if (param == parameters.end()) {
            NES_ERROR("QueryController: Unable to find logicalSourceName for the GET allPhysicalSource request");
            web::json::value errorResponse{};
            errorResponse["detail"] = web::json::value::string("Parameter logicalSourceName must be provided");
            errorMessageImpl(request, errorResponse);
        }

        try {
            std::string logicalSourceName = param->second;
            const std::vector<Catalogs::Source::SourceCatalogEntryPtr>& allPhysicalSource =
                sourceCatalog->getPhysicalSources(logicalSourceName);

            //Prepare the response
            web::json::value result{};
            if (allPhysicalSource.empty()) {
                NES_DEBUG("No Physical Source Found");
                resourceNotFoundImpl(request);
                return;
            }
            std::vector<web::json::value> allSource = {};
            for (auto const& physicalSource : std::as_const(allPhysicalSource)) {
                allSource.push_back(web::json::value::string(physicalSource->toString()));
            }
            result["Physical Sources"] = web::json::value::array(allSource);
            successMessageImpl(request, result);
            return;

        } catch (const std::exception& exc) {
            NES_ERROR("SourceCatalogController: handleGet -allPhysicalSource: Exception occurred while building the "
                      "query plan for user request:"
                      << exc.what());
            handleException(request, exc);
            return;
        } catch (...) {
            NES_ERROR("SourceCatalogController:unknown exception.");
            internalServerErrorImpl(request);
            return;
        }
    } else if (path[1] == "schema") {
        //Check if the path contains the query id
        auto param = parameters.find("logicalSourceName");
        if (param == parameters.end()) {
            NES_ERROR("QueryController: Unable to find logical source name for the GET schema request");
            web::json::value errorResponse{};
            errorResponse["detail"] = web::json::value::string("Parameter logicalSourceName must be provided");
            errorMessageImpl(request, errorResponse);
        }
        try {
            //Prepare Input query from user string
            std::string logicalSourceName = param->second;

            SchemaPtr schema = sourceCatalog->getSchemaForLogicalSource(logicalSourceName);
            SerializableSchemaPtr serializableSchema = SchemaSerializationUtil::serializeSchema(schema, new SerializableSchema());
            std::string msg = serializableSchema->SerializeAsString();
            successMessageImpl(request, msg);
            return;

        } catch (const std::exception& exc) {
            NES_ERROR(
                "SourceCatalogController: handleGet -schema: Exception occurred while retrieving the schema for a logical source"
                << exc.what());
            handleException(request, exc);
            return;
        } catch (...) {
            NES_ERROR("SourceCatalogController:unknown exception.");
            internalServerErrorImpl(request);
            return;
        }
    } else {
        resourceNotFoundImpl(request);
    }
}

void SourceCatalogController::handlePost(const std::vector<utility::string_t>& path, web::http::http_request& message) {

    if (path[1] == "addLogicalSource") {

        NES_DEBUG("SourceCatalogController: handlePost -addLogicalSource: REST received request to add new Logical Source "
                  << message.to_string());
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    NES_DEBUG("SourceCatalogController: handlePost -addLogicalSource: Start trying to add new logical source");
                    //Prepare Input query from user string
                    std::string payload(body.begin(), body.end());
                    NES_DEBUG("SourceCatalogController: handlePost -addLogicalSource: userRequest: " << payload);
                    web::json::value req = web::json::value::parse(payload);
                    NES_DEBUG("SourceCatalogController: handlePost -addLogicalSource: Json Parse Value: " << req);
                    std::string sourceName = req.at("logicalSourceName").as_string();
                    std::string schema = req.at("schema").as_string();
                    NES_DEBUG("SourceCatalogController: handlePost -addLogicalSource: Try to add new Logical Source "
                              << sourceName << " and" << schema);
                    bool added = sourceCatalog->addLogicalSource(sourceName, schema);
                    NES_DEBUG("SourceCatalogController: handlePost -addLogicalSource: Successfully added new logical Source ?"
                              << added);
                    //Prepare the response
                    web::json::value result{};
                    result["success"] = web::json::value::boolean(added);
                    successMessageImpl(message, result);
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR("SourceCatalogController: handlePost -addLogicalSource: Exception occurred while trying to add new "
                              "logical source"
                              << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    NES_ERROR("RestServer: Unable to start REST server unknown exception.");
                    internalServerErrorImpl(message);
                    return;
                }
            })
            .wait();

    } else if (path[1] == "addLogicalSource-ex") {

        NES_DEBUG("SourceCatalogController: handlePost -addLogicalSource: REST received request to add new Logical Source "
                  << message.to_string());
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    // extract protobuf message from http body
                    std::shared_ptr<SerializableNamedSchema> protobufMessage = std::make_shared<SerializableNamedSchema>();

                    if (!protobufMessage->ParseFromArray(body.data(), body.size())) {
                        NES_DEBUG("SourceCatalogController: handlePost -addLogicalSource: invalid Protobuf message");
                        web::json::value errorResponse{};
                        errorResponse["detail"] = web::json::value::string("Invalid Protobuf message");
                        errorMessageImpl(message, errorResponse);
                        return;
                    }

                    NES_DEBUG("SourceCatalogController: handlePost -addLogicalSource: Start trying to add new logical source");
                    // decode protobuf message into c++ obj repr
                    SerializableSchema* schema = protobufMessage->mutable_schema();
                    SchemaPtr deserializedSchema = SchemaSerializationUtil::deserializeSchema(schema);
                    std::string sourceName = protobufMessage->sourcename();

                    // try to add the user supplied source
                    bool added = sourceCatalog->addLogicalSource(sourceName, deserializedSchema);
                    NES_DEBUG("SourceCatalogController: handlePost -addLogicalSource: Successfully added new logical Source ?"
                              << added);

                    if (!added) {
                        web::json::value errorResponse{};
                        errorResponse["detail"] =
                            web::json::value::string("Logical Source name: " + sourceName + " already exists!");
                        errorMessageImpl(message, errorResponse);
                        return;
                    }

                    //forward return value to client
                    web::json::value result{};
                    result["Success"] = web::json::value::boolean(added);
                    successMessageImpl(message, result);
                    return;

                } catch (const std::exception& exc) {
                    NES_ERROR("SourceCatalogController: handlePost -addLogicalSource: Exception occurred while trying to add new "
                              "logical source"
                              << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    NES_ERROR("RestServer: Unable to start REST server unknown exception.");
                    internalServerErrorImpl(message);
                    return;
                }
            })
            .wait();

    } else if (path[1] == "updateLogicalSource") {
        NES_DEBUG("SourceCatalogController: handlePost -updateLogicalSource: REST received request to update Logical Source "
                  << message.to_string());
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    NES_DEBUG("SourceCatalogController: handlePost -updateLogicalSource: Start trying to update logical source");
                    //Prepare Input query from user string
                    std::string userRequest(body.begin(), body.end());
                    NES_DEBUG("SourceCatalogController: handlePost -updateLogicalSource: userRequest: " << userRequest);
                    web::json::value req = web::json::value::parse(userRequest);

                    std::string sourceName = req.at("logicalSourceName").as_string();
                    std::string schema = req.at("schema").as_string();

                    bool updated = sourceCatalog->updatedLogicalSource(sourceName, schema);

                    if (updated) {
                        //Prepare the response
                        web::json::value result{};
                        result["Success"] = web::json::value::boolean(updated);
                        successMessageImpl(message, result);
                    } else {
                        NES_DEBUG("SourceCatalogController: handlePost -updateLogicalSource: unable to find source "
                                  + sourceName);
                        throw std::invalid_argument("Unable to update logical source " + sourceName);
                    }
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR("SourceCatalogController: handlePost -updateLogicalSource: Exception occurred while updating "
                              "Logical Source."
                              << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    NES_ERROR("RestServer: Unable to start REST server unknown exception.");
                    internalServerErrorImpl(message);
                    return;
                }
            })
            .wait();
    } else if (path[1] == "updateLogicalSource-ex") {

        NES_DEBUG("SourceCatalogController: handlePost -updateLogicalSource: REST received request to update Logical Source via "
                  "Protobuf "
                  << message.to_string());
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    // extract protobuf message from http body
                    std::shared_ptr<SerializableNamedSchema> protobufMessage = std::make_shared<SerializableNamedSchema>();

                    if (!protobufMessage->ParseFromArray(body.data(), body.size())) {
                        NES_DEBUG("SourceCatalogController: handlePost -updateLogicalSource: invalid Protobuf message");
                        web::json::value errorResponse{};
                        errorResponse["detail"] = web::json::value::string("Invalid Protobuf message");
                        errorMessageImpl(message, errorResponse);
                        return;
                    }

                    NES_DEBUG("SourceCatalogController: handlePost -updateLogicalSource: Start trying to update logical source");
                    // decode protobuf message into c++ obj repr
                    SerializableSchema* schema = protobufMessage->mutable_schema();
                    SchemaPtr deserializedSchema = SchemaSerializationUtil::deserializeSchema(schema);
                    std::string sourceName = protobufMessage->sourcename();

                    // try to add the user supplied source
                    bool updated = sourceCatalog->updatedLogicalSource(sourceName, deserializedSchema);

                    if (updated) {
                        //Prepare the response
                        web::json::value result{};
                        result["Success"] = web::json::value::boolean(updated);
                        successMessageImpl(message, result);
                    } else {
                        NES_DEBUG("SourceCatalogController: handlePost -updateLogicalSource: unable to find source "
                                  + sourceName);
                        throw std::invalid_argument("Unable to update logical source " + sourceName);
                    }
                    return;

                } catch (const std::exception& exc) {
                    NES_ERROR(
                        "SourceCatalogController: handlePost -updateLogicalSource: Exception occurred while trying to add new "
                        "logical source"
                        << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    NES_ERROR("RestServer: Unable to start REST server unknown exception.");
                    internalServerErrorImpl(message);
                    return;
                }
            })
            .wait();
    } else {
        resourceNotFoundImpl(message);
    }
}

void SourceCatalogController::handleDelete(const std::vector<utility::string_t>& path, web::http::http_request& request) {

    //Extract parameters if any
    auto parameters = getParameters(request);

    if (path[1] == "deleteLogicalSource") {
        //Check if the path contains the query id
        auto param = parameters.find("logicalSourceName");
        if (param == parameters.end()) {
            NES_ERROR("QueryController: Unable to find query ID for the GET execution-plan request");
            web::json::value errorResponse{};
            errorResponse["detail"] = web::json::value::string("Parameter queryId must be provided");
            errorMessageImpl(request, errorResponse);
        }

        try {
            //Prepare Input query from user string
            std::string sourceName = param->second;

            bool added = sourceCatalog->removeLogicalSource(sourceName);

            //Prepare the response
            web::json::value result{};
            if (added) {
                result["Success"] = web::json::value::boolean(added);
                successMessageImpl(request, result);
            } else {
                throw std::invalid_argument("Could not remove logical source " + sourceName);
            }

            return;
        } catch (const std::exception& exc) {
            NES_ERROR("SourceCatalogController: handleDelete -deleteLogicalSource: Exception occurred while building the "
                      "query plan for user request."
                      << exc.what());
            handleException(request, exc);
            return;
        } catch (...) {
            NES_ERROR("RestServer: Unable to start REST server unknown exception.");
            internalServerErrorImpl(request);
            return;
        }
    } else {
        resourceNotFoundImpl(request);
    }
}
}// namespace NES
