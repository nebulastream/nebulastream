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

#include <REST/DTOs/ConnectivityResponse.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>

#include OATPP_CODEGEN_BEGIN(ApiController)

namespace NES {
namespace Catalogs::Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Catalogs::Source
namespace REST {
namespace Controller {
class SourceCatalogController : public oatpp::web::server::api::ApiController {

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     */
    SourceCatalogController(const std::shared_ptr<ObjectMapper>& objectMapper, sourceCatalogPtr sourceCatalog, ErrorHandlerPtr eHandler, oatpp::String completeRouterPrefix)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), sourceCatalog(sourceCatalog), errorHandler(eHandler) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper
     * @return
     */
    static std::shared_ptr<SourceCatalogController> createShared(const std::shared_ptr<ObjectMapper>& objectMapper, sourceCatalogPtr sourceCatalog,  ErrorHandlerPtr errorHandler, std::string routerPrefixAddition){
        oatpp::String completeRouterPrefix = baseRouterPrefix + routerPrefixAddition;
        return std::make_shared<SourceCatalogController>(objectMapper, sourceCatalog, errorHandler, completeRouterPrefix);
    }

    ENDPOINT("GET", "/allLogicalSource", getAllLogicalSource) {
        const std::map<std::string, std::string>& allLogicalSourceAsString = sourceCatalog->getAllLogicalSourceAsString();
        if (allLogicalSourceAsString.empty()) {
            NES_DEBUG("No Logical Source Found");
            return errorHandler->handleError(Status::CODE_404, "Ressource not found.");
        }

    }

  private:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    ErrorHandlerPtr errorHandler;
};
using SourceCatalogControllerPtr = std::shared_ptr<SourceCatalogController>;
}// namespace Controller
}// namespace REST
}// namespace NES

#include OATPP_CODEGEN_END(ApiController)

#endif//NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_SOURCECATALOGCONTROLLER_HPP_
