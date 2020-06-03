#ifndef NESOPTIMIZER_HPP
#define NESOPTIMIZER_HPP

#include <memory>

namespace NES {

class NESExecutionPlan;
typedef std::shared_ptr<NESExecutionPlan> NESExecutionPlanPtr;

class Query;
typedef std::shared_ptr<Query> QueryPtr;

class NESTopologyPlan;
typedef std::shared_ptr<NESTopologyPlan> NESTopologyPlanPtr;

class TranslateFromLegacyPlanPhase;
typedef std::shared_ptr<TranslateFromLegacyPlanPhase> TranslateFromLegacyPlanPhasePtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

/**
 * @brief:
 *         This class is responsible for producing the execution graph for the queries on nes topology. We can have
 *         different type of placement strategies that can be provided as input along with the query and nes topology
 *         information. Based on that a query execution graph will be computed. The output will be a graph where the
 *         edges contain the information about the operators to be executed and the nodes where the execution is to be done.
 */
class NESOptimizer {

  public:
    NESOptimizer();

    /**
     * @brief This method prepares the execution graph for the input user query
     * @param strategy : name of the placement strategy
     * @param inputQuery : the input query for which execution plan is to be prepared
     * @param nesTopologyPlan : The topology of the NEs infrastructure
     * @return Execution plan for the input query
     */
    NESExecutionPlanPtr prepareExecutionGraph(std::string strategy,
                                              QueryPlanPtr queryPlan,
                                              NESTopologyPlanPtr nesTopologyPlan,
                                              StreamCatalogPtr streamCatalog);

  private:
    TranslateFromLegacyPlanPhasePtr translateFromLegacyPlanPhase;
};
}// namespace NES

#endif//NESOPTIMIZER_HPP
