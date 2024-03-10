#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SHARINGIDENTIFICATIONBENCHMARKREQUEST_HPP
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SHARINGIDENTIFICATIONBENCHMARKREQUEST_HPP

#include <Components/NesCoordinator.hpp>
#include <Identifiers.hpp>
#include <RequestProcessor/RequestTypes/AbstractUniRequest.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <nlohmann/json.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

namespace Catalogs {
namespace Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Source
}// namespace Catalogs

namespace RequestProcessor {

//a response to the creator of the request
struct BenchmarkQueryResponse : public AbstractRequestResponse {
    explicit BenchmarkQueryResponse(const nlohmann::json& jsonResponse) : jsonResponse(jsonResponse){};
    nlohmann::json jsonResponse;
};

class SharingIdentificationBenchmarkRequest;
using SharingIdentificationBenchmarkRequestPtr = std::shared_ptr<SharingIdentificationBenchmarkRequest>;

class SharingIdentificationBenchmarkRequest : public AbstractUniRequest {
  public:
    SharingIdentificationBenchmarkRequest(const std::vector<std::string>& queryStrings,
                                          const Optimizer::QueryMergerRule queryMergerRule,
                                          const Optimizer::PlacementStrategy queryPlacementStrategy,
                                          const uint8_t maxRetries,
                                          const z3::ContextPtr& z3Context,
                                          const QueryParsingServicePtr& queryParsingService);

    static SharingIdentificationBenchmarkRequestPtr create(const std::vector<std::string>& queryStrings,
                                                           const Optimizer::QueryMergerRule queryMergerRule,
                                                           const Optimizer::PlacementStrategy queryPlacementStrategy,
                                                           const uint8_t maxRetries,
                                                           const z3::ContextPtr& z3Context,
                                                           const QueryParsingServicePtr& queryParsingService);

  protected:
    /**
     * @brief Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void preRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Roll back any changes made by a request that did not complete due to errors.
     * @param ex: The exception thrown during request execution.
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> rollBack(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) override;

    /**
     * @brief Performs request specific error handling to be done after changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void postRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Executes the request logic.
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> executeRequestLogic(const StorageHandlerPtr& storageHandler) override;

    nlohmann::json getResAsJson(std::vector<SharedQueryPlanPtr> allSQP, float efficiency, long optimizationTime);

  private:
    const std::vector<std::string> queryStrings;
    const Optimizer::QueryMergerRule queryMergerRule;

    QueryId queryId;
    QueryPlanPtr queryPlan;
    Optimizer::PlacementStrategy queryPlacementStrategy;
    z3::ContextPtr z3Context;
    QueryParsingServicePtr queryParsingService;

};//class SharingIdentificationBenchmarkRequest
}// namespace RequestProcessor
}// namespace NES
#endif//NES_SHARINGIDENTIFICATIONBENCHMARKREQUEST_HPP
