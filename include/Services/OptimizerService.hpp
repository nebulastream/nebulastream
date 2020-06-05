#ifndef IMPL_SERVICES_OPTIMIZERSERVICE_H_
#define IMPL_SERVICES_OPTIMIZERSERVICE_H_

#include <API/InputQuery.hpp>
#include <cpprest/json.h>
#include <string>

namespace NES {

class OptimizerService;
typedef std::shared_ptr<OptimizerService> OptimizerServicePtr;

class TopologyManager;
typedef std::shared_ptr<TopologyManager> TopologyManagerPtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class OptimizerService {
  public:
    OptimizerService(TopologyManagerPtr topologyManager);

    /**
     * @brief: get execution plan as json.
     *
     * @param userQuery
     * @param optimizationStrategyName
     * @return
     */
    web::json::value getExecutionPlanAsJson(QueryPlanPtr queryPlan, std::string optimizationStrategyName, StreamCatalogPtr streamCatalog);

    /**
     * @brief: get execution plan for the input query using the specified strategy.
     * @param userQuery
     * @param optimizationStrategyName
     * @return pointer to nes execution plan
     */
    NESExecutionPlanPtr getExecutionPlan(QueryPlanPtr queryPlan, std::string optimizationStrategyName, StreamCatalogPtr streamCatalog);

    ~OptimizerService() = default;

  private:
    TopologyManagerPtr topologyManager;
};

typedef std::shared_ptr<OptimizerService> OptimizerServicePtr;

}// namespace NES

#endif//IMPL_SERVICES_OPTIMIZERSERVICE_H_
