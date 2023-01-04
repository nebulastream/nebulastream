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

#ifndef NES_CORE_INCLUDE_REST_CONTROLLER_SOURCECATALOGCONTROLLER_HPP_
#define NES_CORE_INCLUDE_REST_CONTROLLER_SOURCECATALOGCONTROLLER_HPP_

#include <Catalogs/Source/SourceCatalog.hpp>
#include <Exceptions/MapEntryNotFoundException.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <REST/Controller/BaseRouterPrefix.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <SerializableOperator.pb.h>
#include <nlohmann/json.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/core/parser/Caret.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <utility>

#include OATPP_CODEGEN_BEGIN(ApiController)

namespace NES {
namespace Catalogs::Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Catalogs::Source
class ErrorHandler;
using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;
namespace REST {
namespace Controller {
class SourceCatalogController : public oatpp::web::server::api::ApiController {

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     */
    SourceCatalogController(const std::shared_ptr<ObjectMapper>& objectMapper,
                            Catalogs::Source::SourceCatalogPtr sourceCatalog,
                            ErrorHandlerPtr eHandler,
                            oatpp::String completeRouterPrefix)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), sourceCatalog(sourceCatalog),
          errorHandler(eHandler) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper
     * @return
     */
    static std::shared_ptr<SourceCatalogController> create(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                           Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                                           ErrorHandlerPtr errorHandler,
                                                           std::string routerPrefixAddition) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<SourceCatalogController>(objectMapper, sourceCatalog, errorHandler, completeRouterPrefix);
    }

    ENDPOINT("GET", "/allLogicalSource", getAllLogicalSource) {
        try {
            nlohmann::json logicalSources;
            const std::map<std::string, std::string>& allLogicalSourceAsString = sourceCatalog->getAllLogicalSourceAsString();
            if (allLogicalSourceAsString.empty()) {
                NES_DEBUG("No Logical Source Found");
                return errorHandler->handleError(Status::CODE_404, "Resource not found.");
            }
            for (auto const& [key, val] : allLogicalSourceAsString) {
                nlohmann::json entry;
                entry[key] = val;
                logicalSources.push_back(entry);
            }
            return createResponse(Status::CODE_200, logicalSources.dump());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ADD_CORS(getPhysicalSource)
    ENDPOINT("GET", "/allPhysicalSource", getPhysicalSource, QUERY(String, logicalSourceName, "logicalSourceName")) {
        try {
            const std::vector<Catalogs::Source::SourceCatalogEntryPtr>& allPhysicalSource =
                sourceCatalog->getPhysicalSources(logicalSourceName);

            nlohmann::json result;
            nlohmann::json::array_t allSource = {};
            for (auto const& physicalSource : std::as_const(allPhysicalSource)) {
                allSource.push_back(physicalSource->toString());
            }
            result["Physical Sources"] = allSource;
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

    ADD_CORS(getSchema)
    ENDPOINT("GET", "/schema", getSchema, QUERY(String, logicalSourceName, "logicalSourceName")) {
        try {
            SchemaPtr schema = sourceCatalog->getSchemaForLogicalSource(logicalSourceName);
            SerializableSchemaPtr serializableSchema = SchemaSerializationUtil::serializeSchema(schema, new SerializableSchema());
            return createResponse(Status::CODE_200, serializableSchema->SerializeAsString());
        } catch (const MapEntryNotFoundException& e) {
            return errorHandler->handleError(Status::CODE_404, "Resource Not Found: No Schema found for " + logicalSourceName);
        } catch (const std::exception& exc) {
            NES_ERROR("SourceCatalogController: get schema: Exception occurred while retrieving the schema for a logical source"
                      << exc.what());
            return errorHandler->handleError(Status::CODE_500, exc.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "SourceCatalogController:unknown exception.");
        }
    }

    ADD_CORS(addLogicalSource)
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
            std::string logicalSourceName = reqJson["logicalSourceName"];
            std::string schema = reqJson["schema"];
            NES_DEBUG("SourceCatalogController: addLogicalSource: Try to add new Logical Source " << logicalSourceName << " and "
                                                                                                  << schema);
            bool added = sourceCatalog->addLogicalSource(logicalSourceName, schema);
            NES_DEBUG("SourceCatalogController: addLogicalSource: Successfully added new logical Source ?" << added);
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
                      "logical source"
                      << exc.what());
            return errorHandler->handleError(Status::CODE_500, exc.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "RestServer: Unable to start REST server unknown exception.");
        }
    }

    ADD_CORS(addLogicalSourceEx)
    ENDPOINT("POST", "/addLogicalSource-ex", addLogicalSourceEx, BODY_STRING(String, request)) {

        NES_DEBUG("SourceCatalogController: addLogicalSource: REST received request to add new Logical Source.");
        try {
            std::string req = request.getValue("");
            std::shared_ptr<SerializableNamedSchema> protobufMessage = std::make_shared<SerializableNamedSchema>();

            if (!protobufMessage->ParseFromArray(req.data(), req.size())) {
                NES_DEBUG("SourceCatalogController: handlePost -addLogicalSource: invalid Protobuf message");
                nlohmann::json errorResponse{};
                errorResponse["detail"] = "Invalid Protobuf message";
                return errorHandler->handleError(Status::CODE_400, errorResponse.dump());
            }

            NES_DEBUG("SourceCatalogController: handlePost -addLogicalSource: Start trying to add new logical source");
            // decode protobuf message into c++ obj repr
            SchemaPtr deserializedSchema = SchemaSerializationUtil::deserializeSchema(protobufMessage->schema());
            std::string sourceName = protobufMessage->sourcename();

            // try to add the user supplied source
            bool added = sourceCatalog->addLogicalSource(sourceName, deserializedSchema);
            NES_DEBUG("SourceCatalogController: handlePost -addLogicalSource: Successfully added new logical Source ?" << added);

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
                      "logical source"
                      << exc.what());
            return errorHandler->handleError(Status::CODE_500, exc.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "RestServer: Unable to start REST server unknown exception.");
        }
    }

    ADD_CORS(updateLogicalSource)
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
            std::string sourceName = reqJson["logicalSourceName"];
            std::string schemaName = reqJson["schema"];
            NES_DEBUG("SourceCatalogController: updateLogicalSource: Try to update  Logical Source " << sourceName << " and"
                                                                                                     << schemaName);
            bool updated = sourceCatalog->updatedLogicalSource(sourceName, schemaName);
            NES_DEBUG("SourceCatalogController: addLogicalSource: Successfully added new logical Source ?" << updated);
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
                      "Logical Source."
                      << exc.what());
            return errorHandler->handleError(Status::CODE_500, exc.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "RestServer: Unable to start REST server unknown exception.");
        }
    }

    ADD_CORS(updateLogicalSourceEx)
    ENDPOINT("POST", "/updateLogicalSource-ex", updateLogicalSourceEx, BODY_STRING(String, request)) {

        NES_DEBUG("SourceCatalogController: updateLogicalSource: REST received request to update the given Logical Source.");
        try {
            std::string req = request.getValue("{}");
            //check if json is valid
            if (!nlohmann::json::accept(req)) {
                return errorHandler->handleError(Status::CODE_400, "Invalid JSON");
            }
            std::shared_ptr<SerializableNamedSchema> protobufMessage = std::make_shared<SerializableNamedSchema>();

            if (!protobufMessage->ParseFromArray(req.data(), req.size())) {
                NES_DEBUG("SourceCatalogController: handlePost -updateLogicalSource-ex: invalid Protobuf message");
                nlohmann::json errorResponse{};
                errorResponse["detail"] = "Invalid Protobuf message";
                return errorHandler->handleError(Status::CODE_400, errorResponse.dump());
            }

            NES_DEBUG("SourceCatalogController: handlePost -updateLogicalSource: Start trying to update logical source");
            // decode protobuf message into c++ obj repr
            SchemaPtr deserializedSchema = SchemaSerializationUtil::deserializeSchema(protobufMessage->schema());
            std::string sourceName = protobufMessage->sourcename();

            // try to add the user supplied source
            bool updated = sourceCatalog->updatedLogicalSource(sourceName, deserializedSchema);

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
                      "Logical Source."
                      << exc.what());
            return errorHandler->handleError(Status::CODE_500, exc.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "RestServer: Unable to start REST server unknown exception.");
        }
    }

    ADD_CORS(deleteLogicalSource)
    ENDPOINT("DELETE", "/deleteLogicalSource", deleteLogicalSource, QUERY(String, logicalSourceName, "logicalSourceName")) {
        NES_DEBUG("SourceCatalogController: deleteLogicalSource: REST received request to delete the given Logical Source.");
        try {
            bool deleted = sourceCatalog->removeLogicalSource(logicalSourceName);
            NES_DEBUG("SourceCatalogController: deleteLogicalSource: Successfully deleted the given logical Source: " << deleted);
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

  private:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    ErrorHandlerPtr errorHandler;
};
using SourceCatalogPtr = std::shared_ptr<SourceCatalogController>;
}// namespace Controller
}// namespace REST
}// namespace NES

#include OATPP_CODEGEN_END(ApiController)

#endif// NES_CORE_INCLUDE_REST_CONTROLLER_SOURCECATALOGCONTROLLER_HPP_
