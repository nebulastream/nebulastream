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

#ifndef NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_SOURCECATALOGCONTROLLER_HPP_
#define NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_SOURCECATALOGCONTROLLER_HPP_

#include <REST/DTOs/LogicalSourceResponse.hpp>
#include <REST/DTOs/LogicalSourceInfo.hpp>
#include <REST/DTOs/SourceCatalogStringResponse.hpp>
#include <REST/DTOs/SourceCatalogBoolResponse.hpp>
#include <REST/OatppController/BaseRouterPrefix.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <cpprest/json.h>
#include <REST/Handlers/ErrorHandler.hpp>
#include <SerializableOperator.pb.h>
#include <oatpp/core/parser/Caret.hpp>
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
    SourceCatalogController(const std::shared_ptr<ObjectMapper>& objectMapper, Catalogs::Source::SourceCatalogPtr sourceCatalog, ErrorHandlerPtr eHandler, oatpp::String completeRouterPrefix)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), sourceCatalog(sourceCatalog), errorHandler(eHandler) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper
     * @return
     */
    static std::shared_ptr<SourceCatalogController> createShared(const std::shared_ptr<ObjectMapper>& objectMapper, Catalogs::Source::SourceCatalogPtr sourceCatalog,  ErrorHandlerPtr errorHandler, std::string routerPrefixAddition){
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<SourceCatalogController>(objectMapper, sourceCatalog, errorHandler, completeRouterPrefix);
    }

    ENDPOINT("GET", "/allLogicalSource", getAllLogicalSource) {
        auto dto = LogicalSourceResponse::createShared();
        oatpp::List<oatpp::Object<LogicalSourceInfo>> list({});
        try{
            const std::map<std::string, std::string>& allLogicalSourceAsString = sourceCatalog->getAllLogicalSourceAsString();
            uint64_t count = 0;
            for (auto const& [key, val] : allLogicalSourceAsString) {
                auto entry = LogicalSourceInfo::createShared();
                entry->name = key;
                entry->schema = val;
                count++;
                list->push_back(entry);
            }
            if (count == 0) {
                NES_DEBUG("No Logical Source Found");
                return errorHandler->handleError(Status::CODE_404, "Ressource not found.");
            }
            dto->entries = list;
            return createDtoResponse(Status::CODE_200, dto);
        }
        catch(...){
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("GET",  "/allPhysicalSource",getPhysicalSource, QUERY(String, logicalSourceName, "logicalSourceName")) {
        auto dto = SourceCatalogStringResponse::createShared();
        if (logicalSourceName == "") {
            NES_ERROR("SourceCatalogController: Unable to find logicalSourceName for the GET allPhysicalSource request");
            return errorHandler->handleError(Status::CODE_400, "Bad Request: Parameter logicalSourceName must be provided");
        }
        try {
            const std::vector<Catalogs::Source::SourceCatalogEntryPtr>& allPhysicalSource = sourceCatalog->getPhysicalSources(logicalSourceName);
            if (allPhysicalSource.empty()) {
                NES_DEBUG("No Physical Source Found");
                return errorHandler->handleError(Status::CODE_404, "Resource Not Found: Logical source has no physical source defined.");
            }
            web::json::value result{};
            std::vector<web::json::value> allSource = {};
            for (auto const& physicalSource : std::as_const(allPhysicalSource)) {
                allSource.push_back(web::json::value::string(physicalSource->toString()));
            }
            result["Physical Sources"] = web::json::value::array(allSource);
            dto->sourceInformation = result.to_string();
            return createDtoResponse(Status::CODE_200, dto);
        } catch (const std::exception& exc) {
            NES_ERROR("SourceCatalogController: get allPhysicalSource: Exception occurred while building the query plan for user request.");
            return errorHandler->handleError(Status::CODE_500, exc.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "SourceCatalogController:unknown exception.");
        }
    }

    ENDPOINT("GET",  "schema/{logicalSourceName}", getSchema,
             PATH(String, logicalSourceName)) {
        auto dto = SourceCatalogStringResponse::createShared();
        if (logicalSourceName == "") {
            NES_ERROR("SourceCatalogController: Unable to find logicalSourceName for the GET allPhysicalSource request");
            return errorHandler->handleError(Status::CODE_400, "Bad Request: Parameter logicalSourceName must be provided");
        }
        try {
            SchemaPtr schema = sourceCatalog->getSchemaForLogicalSource(logicalSourceName);
            SerializableSchemaPtr serializableSchema = SchemaSerializationUtil::serializeSchema(schema, new SerializableSchema());
            dto->sourceInformation = serializableSchema->SerializeAsString();
            return createDtoResponse(Status::CODE_200, dto);
        } catch (const std::exception& exc) {
            NES_ERROR(
                "SourceCatalogController: get schema: Exception occurred while retrieving the schema for a logical source"
                << exc.what());
            return errorHandler->handleError(Status::CODE_500, exc.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "SourceCatalogController:unknown exception.");
        }
    }

    ENDPOINT("POST", "addLogicalSource", addLogicalSource,
             BODY_STRING(String, logicalSourceName), BODY_STRING(String, schema)) {
        //OATPP_LOGV(TAG, "POST body %s", body->c_str());
        auto dto = SourceCatalogBoolResponse::createShared();
        NES_DEBUG("SourceCatalogController: addLogicalSource: REST received request to add new Logical Source.");
        try {
            std::string sourceName = logicalSourceName->c_str();
            std::string schemaName = schema->c_str();
            NES_DEBUG("SourceCatalogController: addLogicalSource: Try to add new Logical Source "
                      << sourceName << " and " << schemaName);
            bool added = sourceCatalog->addLogicalSource(logicalSourceName, schema);
            NES_DEBUG("SourceCatalogController: addLogicalSource: Successfully added new logical Source ?"
                      << added);
            //Prepare the response
            if (added){
                dto->success = added;
                return createDtoResponse(Status::CODE_200, dto);
            }
            else{
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
        // TODO Do I need a wait() here?
    }


    ENDPOINT("POST", "updateLogicalSource", updateLogicalSource,
             BODY_STRING(String, logicalSourceName), BODY_STRING(String, schema)) {
        //OATPP_LOGV(TAG, "POST body %s", body->c_str());
        auto dto = SourceCatalogBoolResponse::createShared();
        NES_DEBUG("SourceCatalogController: updateLogicalSource: REST received request to update the given Logical Source.");
        try {
            std::string sourceName = logicalSourceName->c_str();
            std::string schemaName = schema->c_str();
            NES_DEBUG("SourceCatalogController: updateLogicalSource: Try to update  Logical Source "
                       << sourceName << " and" << schemaName);
            bool updated = sourceCatalog->updatedLogicalSource(sourceName, schemaName);
            NES_DEBUG("SourceCatalogController: addLogicalSource: Successfully added new logical Source ?"
                      << updated);
            // Prepare the response
            if (updated){
                dto->success = updated;
                return createDtoResponse(Status::CODE_200, dto);
            }
            else{
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

    ENDPOINT("DELETE", "deleteLogicalSource", deleteLogicalSource,
             PATH(String, logicalSourceName)) {
        if (logicalSourceName == "") {
            NES_ERROR("SourceCatalogController: Unable to find logicalSourceName for the delete request");
            return errorHandler->handleError(Status::CODE_400, "Bad Request: Parameter logicalSourceName must be provided");
        }
        auto dto = SourceCatalogBoolResponse::createShared();
        NES_DEBUG("SourceCatalogController: deleteLogicalSource: REST received request to delete the given Logical Source.");
        try {
            bool deleted = sourceCatalog->removeLogicalSource(logicalSourceName);
            NES_DEBUG("SourceCatalogController: deleteLogicalSource: Successfully deleted the given logical Source: "
                      << deleted);
            // Prepare the response
            if (deleted){
                dto->success = deleted;
                return createDtoResponse(Status::CODE_200, dto);
            }
            else{
                NES_DEBUG("SourceCatalogController: deleteLogicalSource: unable to find given source");
                return errorHandler->handleError(Status::CODE_400, "Unable to delete logical source.");
            }
        } catch (const std::exception& exc) {
            NES_ERROR("SourceCatalogController: deleteLogicalSource: Exception occurred while building the query plan for user request.");
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

#endif//NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_SOURCECATALOGCONTROLLER_HPP_
