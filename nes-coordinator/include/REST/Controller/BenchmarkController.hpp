#ifndef NES_COORDINATOR_INCLUDE_REST_CONTROLLER_BENCHMARKCONTROLLER_HPP
#define NES_COORDINATOR_INCLUDE_REST_CONTROLLER_BENCHMARKCONTROLLER_HPP

#include <Catalogs/Exceptions/InvalidQueryException.hpp>
#include <Exceptions/MapEntryNotFoundException.hpp>
#include <Operators/Serialization/QueryPlanSerializationUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <REST/Controller/BaseRouterPrefix.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <SerializableQueryPlan.pb.h>
#include <Services/RequestHandlerService.hpp>
#include <exception>
#include <iostream>
#include <memory>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <string>
#include <utility>

#include OATPP_CODEGEN_BEGIN(ApiController)


namespace NES {
class NesCoordinator;
using NesCoordinatorWeakPtr = std::weak_ptr<NesCoordinator>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class ErrorHandler;
using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class QueryService;
using QueryServicePtr = std::shared_ptr<QueryService>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class ErrorHandler;
using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;

namespace REST::Controller {
class BenchmarkController : public oatpp::web::server::api::ApiController {//BenchmarkController 继承了 ApiController

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     */
    BenchmarkController(const std::shared_ptr<ObjectMapper>& objectMapper,
                        const RequestHandlerServicePtr& requestHandlerService,
                        const QueryCatalogServicePtr& queryCatalogService,
                        const GlobalQueryPlanPtr& globalQueryPlan,
                        const Optimizer::GlobalExecutionPlanPtr& globalExecutionPlan,
                        const std::string& completeRouterPrefix,
                        const ErrorHandlerPtr& errorHandler)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix),
          requestHandlerService(requestHandlerService), queryCatalogService(queryCatalogService),
          globalQueryPlan(globalQueryPlan), globalExecutionPlan(globalExecutionPlan), errorHandler(errorHandler) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper
     * @return
     */
    static std::shared_ptr<BenchmarkController> create(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                       const RequestHandlerServicePtr& requestHandlerService,
                                                       const QueryCatalogServicePtr& queryCatalogService,
                                                       const GlobalQueryPlanPtr& globalQueryPlan,
                                                       const Optimizer::GlobalExecutionPlanPtr& globalExecutionPlan,
                                                       const std::string& routerPrefixAddition,
                                                       const ErrorHandlerPtr& errorHandler) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<BenchmarkController>(objectMapper,
                                                     requestHandlerService,
                                                     queryCatalogService,
                                                     globalQueryPlan,
                                                     globalExecutionPlan,
                                                     completeRouterPrefix,
                                                     errorHandler);
    }

    // (HTTP method, path,name,para)
    ENDPOINT("POST", "/test", test, BODY_STRING(String, request)) {

        NES_INFO("receive request with parameters");
        try {
            std::string req = request.getValue("{}");
            nlohmann::json requestJson = nlohmann::json::parse(req);

//            const auto workloadType = requestJson["workloadType"].get<std::string>();
//            const auto noOfQueries = requestJson["noOfQueries"].get<uint64_t>();
            const std::vector<std::string> queryStrings = requestJson["queries"].get<std::vector<std::string>>();
            const auto queryMergerRule = static_cast<Optimizer::QueryMergerRule>(requestJson["queryMergerRule"].get<uint8_t>());

            //send the queryset from request

            const auto response = requestHandlerService->validateAndQueueSharingIdentificationBenchmarkRequest(
                queryStrings,
                queryMergerRule,
                Optimizer::PlacementStrategy::TopDown);

            return createResponse(Status::CODE_200, response.dump());//serialize

        } catch (const InvalidQueryException& exc) {
            NES_ERROR("QueryController: handlePost -execute-query: Exception occurred during submission of a query "
                      "user request: {}",
                      exc.what());
            return errorHandler->handleError(Status::CODE_400, exc.what());
        } catch (const MapEntryNotFoundException& exc) {
            NES_ERROR("QueryController: handlePost -execute-query: Exception occurred during submission of a query "
                      "user request: {}",
                      exc.what());
            return errorHandler->handleError(Status::CODE_400, exc.what());
        } catch (nlohmann::json::exception& e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Server Error");
        }
    }

  private:
    RequestHandlerServicePtr requestHandlerService;
    QueryCatalogServicePtr queryCatalogService;
    GlobalQueryPlanPtr globalQueryPlan;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    ErrorHandlerPtr errorHandler;
};

}// namespace REST::Controller

}// namespace NES
#endif//NES_COORDINATOR_INCLUDE_REST_CONTROLLER_BENCHMARKCONTROLLER_HPP
