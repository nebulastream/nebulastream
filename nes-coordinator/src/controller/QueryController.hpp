#ifndef QueryController_hpp
#define QueryController_hpp

#include "oatpp/web/server/api/ApiController.hpp"
#include "oatpp/core/macro/codegen.hpp"
#include "oatpp/core/macro/component.hpp"

#include OATPP_CODEGEN_BEGIN(ApiController) ///< Begin Codegen

/**
 * Sample Api Controller.
 */
class QueryController : public oatpp::web::server::api::ApiController {
public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     */
    QueryController(const std::shared_ptr<ObjectMapper>& objectMapper)
        : oatpp::web::server::api::ApiController(objectMapper) {}
public:

    ENDPOINT("GET", "/query", root) {
        return createResponse(Status::CODE_200, "Hello, oatpp!");
    }

};

#include OATPP_CODEGEN_END(ApiController) ///< End Codegen

#endif /* QueryController_hpp */
