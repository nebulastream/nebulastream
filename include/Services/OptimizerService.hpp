#ifndef IMPL_SERVICES_OPTIMIZERSERVICE_H_
#define IMPL_SERVICES_OPTIMIZERSERVICE_H_

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

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

class OptimizerService {
  public:
    OptimizerService(TopologyManagerPtr topologyManager, StreamCatalogPtr streamCatalog, GlobalExecutionPlanPtr globalExecutionPlan);

    /**
     * @brief: get execution plan as string.
     *
     * @param queryPlan : the logical plan of the query
     * @param optimizationStrategyName : name of the strategy used for preparing the execution plan
     * @return execution plan as string representation
     */
    std::string getExecutionPlanAsString(QueryPlanPtr queryPlan, std::string optimizationStrategyName);

    /**
     * @brief: get execution plan for the input query using the specified strategy.
     * @param userQuery
     * @param optimizationStrategyName
     * @return pointer to nes execution plan
     */
    GlobalExecutionPlanPtr updateGlobalExecutionPlan(QueryPlanPtr queryPlan, std::string optimizationStrategyName);

    ~OptimizerService() = default;

  private:
    TopologyManagerPtr topologyManager;
    StreamCatalogPtr streamCatalog;
    GlobalExecutionPlanPtr globalExecutionPlan;
};

typedef std::shared_ptr<OptimizerService> OptimizerServicePtr;

}// namespace NES

#endif//IMPL_SERVICES_OPTIMIZERSERVICE_H_
