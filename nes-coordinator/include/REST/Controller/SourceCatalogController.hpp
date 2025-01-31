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

#ifndef NES_COORDINATOR_INCLUDE_REST_CONTROLLER_SOURCECATALOGCONTROLLER_HPP_
#define NES_COORDINATOR_INCLUDE_REST_CONTROLLER_SOURCECATALOGCONTROLLER_HPP_

#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Exceptions/MapEntryNotFoundException.hpp>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <REST/Controller/BaseRouterPrefix.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <SerializableOperator.pb.h>
#include <Services/QueryParsingService.hpp>
#include <Services/RequestHandlerService.hpp>
#include <nlohmann/json.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/core/parser/Caret.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <utility>

#include OATPP_CODEGEN_BEGIN(ApiController)

namespace NES::REST::Controller {
class SourceCatalogController : public oatpp::web::server::api::ApiController {

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     */
    SourceCatalogController(const std::shared_ptr<ObjectMapper>& objectMapper,
                            const RequestHandlerServicePtr requestHandlerService,
                            const QueryParsingServicePtr& queryParsingService,
                            const ErrorHandlerPtr& eHandler,
                            const oatpp::String& completeRouterPrefix)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), queryParsingService(queryParsingService),
          errorHandler(eHandler), requestHandlerService(requestHandlerService) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper
     * @return
     */
    static std::shared_ptr<SourceCatalogController> create(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                           const RequestHandlerServicePtr requestHandlerService,
                                                           const QueryParsingServicePtr& queryParsingService,
                                                           const ErrorHandlerPtr& errorHandler,
                                                           const std::string& routerPrefixAddition) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<SourceCatalogController>(objectMapper,
                                                         requestHandlerService,
                                                         queryParsingService,
                                                         errorHandler,
                                                         completeRouterPrefix);
    }

    ENDPOINT("GET", "/allLogicalSource", getAllLogicalSource) {
        try {
            auto allLogicalSources = requestHandlerService->queueGetAllLogicalSourcesRequest();
            if (allLogicalSources.empty()) {
                NES_DEBUG("No Logical Source Found");
                return errorHandler->handleError(Status::CODE_404, "Resource not found.");
            }
            return createResponse(Status::CODE_200, allLogicalSources.dump());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("GET", "/allPhysicalSource", getPhysicalSource, QUERY(String, logicalSourceName, "logicalSourceName")) {
        try {
            auto result = requestHandlerService->queueGetPhysicalSourcesRequest(logicalSourceName);
            return createResponse(Status::CODE_200, result.dump());
        } catch (const MapEntryNotFoundException& e) {
            return errorHandler->handleError(Status::CODE_404,
                                             "Resource Not Found: Logical source " + logicalSourceName
                                                 + " has no physical source defined.");
        } catch (const std::exception& exc) {
            NES_ERROR("SourceCatalogController: get allPhysicalSource: Exception occurred while building the query plan for user "
                      "request.");
            return errorHandler->handleError(Status::CODE_500, exc.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "SourceCatalogController:unknown exception.");
        }
    }

    ENDPOINT("GET", "/schema", getSchema, QUERY(String, logicalSourceName, "logicalSourceName")) {
        try {
            auto schema = requestHandlerService->queueGetLogicalSourceSchemaRequest(logicalSourceName);
            auto serializableSchema = SchemaSerializationUtil::serializeSchema(schema, new SerializableSchema());
            auto string = serializableSchema->SerializeAsString();
            return createResponse(Status::CODE_200, string);
        } catch (const MapEntryNotFoundException& e) {
            return errorHandler->handleError(Status::CODE_404, "Resource Not Found: No Schema found for " + logicalSourceName);
        } catch (const std::exception& exc) {
            NES_ERROR(
                "SourceCatalogController: get schema: Exception occurred while retrieving the schema for a logical source {}",
                exc.what());
            return errorHandler->handleError(Status::CODE_500, exc.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "SourceCatalogController:unknown exception.");
        }
    }

    ENDPOINT("POST", "/addLogicalSource", addLogicalSource, BODY_STRING(String, request)) {

        NES_DEBUG("SourceCatalogController: addLogicalSource: REST received request to add new Logical Source.");
        try {
            std::string req = request.getValue("{}");
            //check if json is valid
            if (!nlohmann::json::accept(req)) {
                return errorHandler->handleError(Status::CODE_400, "Invalid JSON");
            }
            //validate post request body
            nlohmann::json reqJson = nlohmann::json::parse(req);
            if (!reqJson.contains("logicalSourceName")) {
                return errorHandler->handleError(Status::CODE_400, "Request body must contain 'logicalSourceName'");
            }
            if (!reqJson.contains("schema")) {
                return errorHandler->handleError(Status::CODE_400, "Request body must contain 'schema'");
            }
            auto logicalSourceName = reqJson["logicalSourceName"];
            auto schemaString = reqJson["schema"];
            NES_DEBUG("SourceCatalogController: addLogicalSource: Try to add new Logical Source {} and {}",
                      logicalSourceName,
                      schemaString);
            auto schema = queryParsingService->createSchemaFromCode(schemaString);
            bool added = requestHandlerService->queueRegisterLogicalSourceRequest(logicalSourceName, schema);
            NES_DEBUG("SourceCatalogController: addLogicalSource: Successfully added new logical Source ? {}", added);
            //Prepare the response
            if (added) {
                nlohmann::json success;
                success["success"] = added;
                return createResponse(Status::CODE_200, success.dump());
            } else {
                return errorHandler->handleError(Status::CODE_400, "Logical Source with same name already exists!");
            }
        } catch (const std::exception& exc) {
            NES_ERROR("SourceCatalogController: addLogicalSource: Exception occurred while trying to add new "
                      "logical source {}",
                      exc.what());
            return errorHandler->handleError(Status::CODE_500, exc.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "RestServer: Unable to start REST server unknown exception.");
        }
    }

    ENDPOINT("POST", "/addLogicalSource-ex", addLogicalSourceEx, BODY_STRING(String, request)) {

        NES_DEBUG("SourceCatalogController: addLogicalSource: REST received request to add new Logical Source.");
        try {
            std::string req = request.getValue("");
            auto protobufMessage = std::make_shared<SerializableNamedSchema>();

            if (!protobufMessage->ParseFromArray(req.data(), req.size())) {
                NES_DEBUG("SourceCatalogController: handlePost -addLogicalSource: invalid Protobuf message");
                nlohmann::json errorResponse{};
                errorResponse["detail"] = "Invalid Protobuf message";
                return errorHandler->handleError(Status::CODE_400, errorResponse.dump());
            }

            NES_DEBUG("SourceCatalogController: handlePost -addLogicalSource: Start trying to add new logical source");
            // decode protobuf message into c++ obj repr
            auto deserializedSchema = SchemaSerializationUtil::deserializeSchema(protobufMessage->schema());
            auto sourceName = protobufMessage->sourcename();

            // try to add the user supplied source
            bool added = requestHandlerService->queueRegisterLogicalSourceRequest(sourceName, deserializedSchema);
            NES_DEBUG("SourceCatalogController: handlePost -addLogicalSource: Successfully added new logical Source ? {}", added);

            if (!added) {
                nlohmann::json errorResponse{};
                errorResponse["detail"] = "Logical Source name: " + sourceName + " already exists!";
                return errorHandler->handleError(Status::CODE_400, errorResponse.dump());
            }

            //forward return value to client
            nlohmann::json result{};
            result["success"] = added;
            return createResponse(Status::CODE_200, result.dump());
        } catch (const std::exception& exc) {
            NES_ERROR("SourceCatalogController: addLogicalSource-ex: Exception occurred while trying to add new "
                      "logical source {}",
                      exc.what());
            return errorHandler->handleError(Status::CODE_500, exc.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "RestServer: Unable to start REST server unknown exception.");
        }
    }

    ENDPOINT("POST", "/updateLogicalSource", updateLogicalSource, BODY_STRING(String, request)) {

        NES_DEBUG("SourceCatalogController: updateLogicalSource: REST received request to update the given Logical Source.");
        try {
            std::string req = request.getValue("{}");
            //check if json is valid
            if (!nlohmann::json::accept(req)) {
                return errorHandler->handleError(Status::CODE_400, "Invalid JSON");
            }
            //validate post request body
            nlohmann::json reqJson = nlohmann::json::parse(req);
            if (!reqJson.contains("logicalSourceName")) {
                return errorHandler->handleError(Status::CODE_400, "Request body must contain 'logicalSourceName'");
            }
            if (!reqJson.contains("schema")) {
                return errorHandler->handleError(Status::CODE_400, "Request body must contain 'schema'");
            }
            auto sourceName = reqJson["logicalSourceName"];
            auto schemaString = reqJson["schema"];
            NES_DEBUG("SourceCatalogController: updateLogicalSource: Try to update  Logical Source {} with schema {}",
                      sourceName,
                      schemaString);
            auto schema = queryParsingService->createSchemaFromCode(schemaString);
            auto updated = requestHandlerService->queueUpdateLogicalSourceRequest(sourceName, schema);
            NES_DEBUG("SourceCatalogController: addLogicalSource: Successfully added new logical Source ? {}", updated);
            // Prepare the response
            if (updated) {
                nlohmann::json success;
                success["success"] = updated;
                return createResponse(Status::CODE_200, success.dump());
            } else {
                NES_DEBUG("SourceCatalogController: updateLogicalSource: unable to find given source");
                return errorHandler->handleError(Status::CODE_400, "Unable to update logical source.");
            }
        } catch (const std::exception& exc) {
            NES_ERROR("SourceCatalogController: updateLogicalSource: Exception occurred while updating "
                      "Logical Source {}",
                      exc.what());
            return errorHandler->handleError(Status::CODE_500, exc.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "RestServer: Unable to start REST server unknown exception.");
        }
    }

    ENDPOINT("POST", "/updateLogicalSource-ex", updateLogicalSourceEx, BODY_STRING(String, request)) {

        NES_DEBUG("SourceCatalogController: updateLogicalSource: REST received request to update the given Logical Source.");
        try {
            std::string req = request.getValue("{}");
            //check if json is valid
            if (!nlohmann::json::accept(req)) {
                return errorHandler->handleError(Status::CODE_400, "Invalid JSON");
            }
            auto protobufMessage = std::make_shared<SerializableNamedSchema>();

            if (!protobufMessage->ParseFromArray(req.data(), req.size())) {
                NES_DEBUG("SourceCatalogController: handlePost -updateLogicalSource-ex: invalid Protobuf message");
                nlohmann::json errorResponse{};
                errorResponse["detail"] = "Invalid Protobuf message";
                return errorHandler->handleError(Status::CODE_400, errorResponse.dump());
            }

            NES_DEBUG("SourceCatalogController: handlePost -updateLogicalSource: Start trying to update logical source");
            // decode protobuf message into c++ obj repr
            auto deserializedSchema = SchemaSerializationUtil::deserializeSchema(protobufMessage->schema());
            auto sourceName = protobufMessage->sourcename();

            // try to add the user supplied source
            auto updated = requestHandlerService->queueUpdateLogicalSourceRequest(sourceName, deserializedSchema);

            if (updated) {
                //Prepare the response
                nlohmann::json result{};
                result["success"] = updated;
                return createResponse(Status::CODE_200, result.dump());
            } else {
                nlohmann::json errorResponse{};
                errorResponse["detail"] = "Unable to update logical source " + sourceName;
                return errorHandler->handleError(Status::CODE_400, errorResponse.dump());
            }
        } catch (const std::exception& exc) {
            NES_ERROR("SourceCatalogController: updateLogicalSource: Exception occurred while updating "
                      "Logical Source. {}",
                      exc.what());
            return errorHandler->handleError(Status::CODE_500, exc.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "RestServer: Unable to start REST server unknown exception.");
        }
    }

    ENDPOINT("DELETE", "/deleteLogicalSource", deleteLogicalSource, QUERY(String, logicalSourceName, "logicalSourceName")) {
        NES_DEBUG("SourceCatalogController: deleteLogicalSource: REST received request to delete the given Logical Source.");
        try {
            auto deleted = requestHandlerService->queueUnregisterLogicalSourceRequest(logicalSourceName);
            NES_DEBUG("SourceCatalogController: deleteLogicalSource: Successfully deleted the given logical Source: {}", deleted);
            // Prepare the response
            if (deleted) {
                nlohmann::json success;
                success["success"] = deleted;
                return createResponse(Status::CODE_200, success.dump());
            } else {
                NES_DEBUG("SourceCatalogController: deleteLogicalSource: unable to find given source");
                return errorHandler->handleError(Status::CODE_400,
                                                 "Unable to delete logical source. Either logical source doesnt exist or"
                                                 " there are still physical sources mapped to the logical source");
            }
        } catch (const std::exception& exc) {
            NES_ERROR("SourceCatalogController: deleteLogicalSource: Exception occurred while building the query plan for user "
                      "request.");
            return errorHandler->handleError(Status::CODE_500, exc.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "SourceCatalogController:unknown exception.");
        }
    }

    ENDPOINT("POST", "/addSourceStatistics", addSourceStatistics, BODY_STRING(String, request)) {
        NES_DEBUG("SourceCatalogController: addSourceStatistics: REST received request to add source statistics.");
        try {
            std::string req = request.getValue("{}");
            //check if json is valid
            if (!nlohmann::json::accept(req)) {
                return errorHandler->handleError(Status::CODE_400, "Invalid JSON");
            }
            auto protobufMessage = std::make_shared<SerializableNamedSchema>();

            auto jsonFields = nlohmann::json::parse(req);
            WorkerId workerId = jsonFields["topologyNodeId"];
            std::string logSourceName = jsonFields["logicalSource"];
            std::string phSourceName = jsonFields["physicalSource"];
            std::string fieldName = jsonFields["fieldName"];
            std::string value = jsonFields["value"];
            bool success = false;
            success = requestHandlerService->queueAddKeyDistributionEntryRequest(logSourceName, phSourceName, workerId, value);
            nlohmann::json successJson;
            successJson["success"] = success;
            if (success) {
                return createResponse(Status::CODE_200, successJson.dump());
            } else {
                return createResponse(Status::CODE_500,
                                      "SourceCatalogController: addSourceStatistics: failed with success=" + successJson.dump());
            }
        } catch (const std::exception& exc) {
            NES_ERROR("SourceCatalogController: addSourceStatistics: Exception occurred {}", exc.what());
            return errorHandler->handleError(Status::CODE_500, exc.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "RestServer: Unable to start REST server unknown exception.");
        }
    }

  private:
    QueryParsingServicePtr queryParsingService;
    ErrorHandlerPtr errorHandler;
    RequestHandlerServicePtr requestHandlerService;
};
using SourceCatalogPtr = std::shared_ptr<Catalogs::Source::SourceCatalog>;
}// namespace NES::REST::Controller

#include OATPP_CODEGEN_END(ApiController)

#endif// NES_COORDINATOR_INCLUDE_REST_CONTROLLER_SOURCECATALOGCONTROLLER_HPP_
