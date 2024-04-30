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

#ifndef NES_ISQPREQUEST_HPP
#define NES_ISQPREQUEST_HPP

#include <RequestProcessor/RequestTypes/AbstractUniRequest.hpp>
#include <thread>

namespace NES {

namespace Statistic {
class StatisticProbeHandler;
using StatisticProbeHandlerPtr = std::shared_ptr<StatisticProbeHandler>;
}// namespace Statistic

namespace Optimizer {
class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;
}// namespace Optimizer

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class SharedQueryPlan;
using SharedQueryPlanPtr = std::shared_ptr<SharedQueryPlan>;

namespace Catalogs::Query {
class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}// namespace Catalogs::Query

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

namespace RequestProcessor {

class ISQPRequest;
using ISQPRequestPtr = std::shared_ptr<ISQPRequest>;

class ISQPEvent;
using ISQPEventPtr = std::shared_ptr<ISQPEvent>;

class ISQPAddQueryEvent;
using ISQPAddQueryEventPtr = std::shared_ptr<ISQPAddQueryEvent>;

class ISQPRemoveQueryEvent;
using ISQPRemoveQueryEventPtr = std::shared_ptr<ISQPRemoveQueryEvent>;

class ISQPRemoveNodeEvent;
using ISQPRemoveNodeEventPtr = std::shared_ptr<ISQPRemoveNodeEvent>;

class ISQPRemoveLinkEvent;
using ISQPRemoveLinkEventPtr = std::shared_ptr<ISQPRemoveLinkEvent>;

/**
 * @brief Response to the execution of the ISQP request with the success, start time, and end time.
 */
struct ISQPRequestResponse : AbstractRequestResponse {
    explicit ISQPRequestResponse(uint64_t processingStartTime,
                                 uint64_t amendmentStartTime,
                                 uint64_t processingEndTime,
                                 uint64_t numOfSQPAffected,
                                 uint64_t numOfFailedPlacements,
                                 bool success)
        : processingStartTime(processingStartTime), amendmentStartTime(amendmentStartTime), processingEndTime(processingEndTime),
          numOfSQPAffected(numOfSQPAffected), numOfFailedPlacements(numOfFailedPlacements), success(success){};
    uint64_t processingStartTime;
    uint64_t amendmentStartTime;
    uint64_t processingEndTime;
    uint64_t numOfSQPAffected;
    uint64_t numOfFailedPlacements;
    bool success;
};
using ISQPRequestResponsePtr = std::shared_ptr<ISQPRequestResponse>;

class PlacementAmendmentInstance;
using PlacementAmendmentInstancePtr = std::shared_ptr<PlacementAmendmentInstance>;

/**
 * @brief This is a meta request that is capable of handling a batch of external events.
 * For example, the system can either execute 10 individual add query requests or one ISQPRequest with a batch of 10 ISQPAddQueryEvents.
 * Both will result in deployment of 10 queries. However, batch based deployment will result in a lower deployment latency.
 * Similarly, we can submit an arbitrary batch consisting of AddQuery, RemoveQuery, AddNode, RemoveNode, AddLink, or RemoveLink events.
 */
class ISQPRequest : public AbstractUniRequest {

  public:
    ISQPRequest(const z3::ContextPtr& z3Context, std::vector<ISQPEventPtr> events, uint8_t maxRetries);

    static ISQPRequestPtr create(const z3::ContextPtr& z3Context, std::vector<ISQPEventPtr> events, uint8_t maxRetries);

  protected:
    std::vector<AbstractRequestPtr> executeRequestLogic(const StorageHandlerPtr& storageHandle) override;

  public:
    std::vector<AbstractRequestPtr> rollBack(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) override;

  protected:
    void preRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) override;
    void postRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) override;

  private:
    /**
     * @brief run a placement amendment instance from the queue
     */
    void runPlacementAmenderInstance();

    /**
     * @brief handle add query event by marking the affected operators for placement
     * @param addQueryEvent
     */
    QueryId handleAddQueryRequest(ISQPAddQueryEventPtr addQueryEvent);

    /**
     * @brief handle remove query event by marking the affected operators for removal
     * @param removeQueryEvent
     */
    void handleRemoveQueryRequest(ISQPRemoveQueryEventPtr removeQueryEvent);

    /**
     * @brief handle remove node event by marking the affected operators for replacement
     * @param removeNodeEvent
     */
    void handleRemoveNodeRequest(ISQPRemoveNodeEventPtr removeNodeEvent);

    /**
     * @brief handle remove link event by marking the affected operators for replacement
     * @param removeLinkEvent
     */
    void handleRemoveLinkRequest(ISQPRemoveLinkEventPtr removeLinkEvent);

    z3::ContextPtr z3Context;
    std::vector<ISQPEventPtr> events;
    TopologyPtr topology;
    GlobalQueryPlanPtr globalQueryPlan;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Configurations::CoordinatorConfigurationPtr coordinatorConfiguration;
    Statistic::StatisticProbeHandlerPtr statisticProbeHandler;
};

}// namespace RequestProcessor
}// namespace NES

#endif//NES_ISQPREQUEST_HPP
