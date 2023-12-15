//
// Created by ninghong on 12/5/23.
//

#ifndef NES_BENCHMARKCONTROLLER_HPP
#define NES_BENCHMARKCONTROLLER_HPP




namespace NES {
class NesCoordinator;//声明一下class NesCoordinator的存在
using NesCoordinatorWeakPtr = std::weak_ptr<NesCoordinator>;  // NesCoordinatorWeakPtr 是指针类型

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
class BenchmarkController : public oatpp::web::server::api::ApiController {  //BenchmarkController 继承了 ApiController

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     */
    BenchmarkController(const std::shared_ptr<ObjectMapper>& objectMapper,
                    const QueryServicePtr& queryService,
                    const QueryCatalogServicePtr& queryCatalogService,
                    const GlobalQueryPlanPtr& globalQueryPlan,
                    const GlobalExecutionPlanPtr& globalExecutionPlan,
                    const std::string& completeRouterPrefix,
                    const ErrorHandlerPtr& errorHandler)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), queryService(queryService),
          queryCatalogService(queryCatalogService), globalQueryPlan(globalQueryPlan), globalExecutionPlan(globalExecutionPlan),
          errorHandler(errorHandler) {} //：后面是初始化列表；{} 里写的函数体 为空

    /**
     * Create a shared object of the API controller
     * @param objectMapper
     * @return
     */
    static std::shared_ptr<QueryController> create(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                   const QueryServicePtr& queryService,
                                                   const QueryCatalogServicePtr& queryCatalogService,
                                                   const GlobalQueryPlanPtr& globalQueryPlan,
                                                   const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                   const std::string& routerPrefixAddition,
                                                   const ErrorHandlerPtr& errorHandler) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<QueryController>(objectMapper,
                                                 queryService,
                                                 queryCatalogService,
                                                 globalQueryPlan,
                                                 globalExecutionPlan,
                                                 completeRouterPrefix,
                                                 errorHandler);
    }

    // (HTTP method, path,name,para)
    ENDPOINT("GET", "/test", test, QUERY(UInt64, queryId, "queryId")) {
        try {
            return createResponse(Status::CODE_200, "success");
        } catch (Exceptions::QueryNotFoundException& e) {
            return errorHandler->handleError(Status::CODE_404, "No query with given ID: " + std::to_string(queryId));
        } catch (nlohmann::json::exception& e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }





  private:
    QueryServicePtr queryService;
    QueryCatalogServicePtr queryCatalogService;
    GlobalQueryPlanPtr globalQueryPlan;
    GlobalExecutionPlanPtr globalExecutionPlan;
    ErrorHandlerPtr errorHandler;


};// namespace REST::Controller

}// namespace NES





#endif//NES_BENCHMARKCONTROLLER_HPP

