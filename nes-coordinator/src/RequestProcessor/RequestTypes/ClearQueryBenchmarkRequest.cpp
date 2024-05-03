#include <RequestProcessor/RequestTypes/ClearQueryBenchmarkRequest.hpp>

#include <Exceptions/RequestExecutionException.hpp>

#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>

#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>

// TODO: reorganize header files

namespace NES::RequestProcessor {

ClearQueryBenchmarkRequest::ClearQueryBenchmarkRequest(const uint8_t maxRetries)
    : AbstractUniRequest({ResourceType::QueryCatalogService,
                          ResourceType::GlobalExecutionPlan,
                          ResourceType::Topology,
                          ResourceType::GlobalQueryPlan,
                          ResourceType::UdfCatalog,
                          ResourceType::SourceCatalog,
                          ResourceType::CoordinatorConfiguration},
                         maxRetries) {}

ClearQueryBenchmarkRequestPtr ClearQueryBenchmarkRequest::create(const uint8_t maxRetries) {
    return std::make_shared<ClearQueryBenchmarkRequest>(maxRetries);
}

void ClearQueryBenchmarkRequest::preRollbackHandle([[maybe_unused]] std::exception_ptr ex,
                                                   [[maybe_unused]] const StorageHandlerPtr& storageHandler) {}

std::vector<AbstractRequestPtr> ClearQueryBenchmarkRequest::rollBack([[maybe_unused]] std::exception_ptr exception,
                                                                     [[maybe_unused]] const StorageHandlerPtr& storageHandler) {
    return {};
}

void ClearQueryBenchmarkRequest::postRollbackHandle([[maybe_unused]] std::exception_ptr exception,
                                                    [[maybe_unused]] const StorageHandlerPtr& storageHandler) {}

std::vector<AbstractRequestPtr> ClearQueryBenchmarkRequest::executeRequestLogic(const StorageHandlerPtr& storageHandler) {
    try {
        NES_DEBUG("Acquiring required resources.");
        // Acquire all necessary resources
        // TODO: check globalExecutionPlan
        // auto globalExecutionPlan = storageHandler->getGlobalExecutionPlanHandle(requestId);
        auto queryCatalogService = storageHandler->getQueryCatalogServiceHandle(requestId);
        auto globalQueryPlan = storageHandler->getGlobalQueryPlanHandle(requestId);

        //clear old queries after one query-merging run
        queryCatalogService->clearQueries();
        globalQueryPlan->reset();
        responsePromise.set_value(std::make_shared<ClearQueryBenchmarkResponse>(true));

    } catch (RequestExecutionException& exception) {
        NES_ERROR("Exception occurred while processing AddQueryRequest with error {}", exception.what());
        handleError(std::current_exception(), storageHandler);
    }

    return {};
}

}// namespace NES::RequestProcessor