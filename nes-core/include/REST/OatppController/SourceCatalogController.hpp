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
        oatpp::String completeRouterPrefix = baseRouterPrefix + routerPrefixAddition;
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

    ENDPOINT("GET",  "allPhysicalSource/{logicalSourceName}", getPhysicalSource,
             PATH(String, logicalSourceName)) {
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
            NES_ERROR("SourceCatalogController: handleGet -allPhysicalSource: Exception occurred while building the query plan for user request.");
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
                "SourceCatalogController: handleGet -schema: Exception occurred while retrieving the schema for a logical source"
                << exc.what());
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
