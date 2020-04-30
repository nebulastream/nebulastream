
/**\brief:
 *         This class is responsible for producing the execution graph for the queries on nes topology. We can have
 *         different type of optimizers that can be provided as input the query and nes topology information and based
 *         on that a query execution graph will be computed. The output will be a graph where the edges contain the
 *         information about the operators to be executed and the nodes where the execution is to be done.
 */

#ifndef NESOPTIMIZER_HPP
#define NESOPTIMIZER_HPP

#include <memory>

namespace NES {

class NESExecutionPlan;
typedef std::shared_ptr<NESExecutionPlan> NESExecutionPlanPtr;

class InputQuery;
typedef std::shared_ptr<InputQuery> InputQueryPtr;

class NESTopologyPlan;
typedef std::shared_ptr<NESTopologyPlan> NESTopologyPlanPtr;

/**
 * @brief This class is responsible for creating the NES execution plan after performing query-rewrite, and
 * query-placement.
 */
class NESOptimizer {

  public:

    /**
     * @brief This method prepares the execution graph for the input user query
     * @param strategy : name of the placement strategy
     * @param inputQuery : the input query for which execution plan is to be prepared
     * @param nesTopologyPlan : The topology of the NEs infrastructure
     * @return Execution plan for the input query
     */
    NESExecutionPlanPtr prepareExecutionGraph(std::string strategy,
                                              InputQueryPtr inputQuery,
                                              NESTopologyPlanPtr nesTopologyPlan);
};
}

#endif //NESOPTIMIZER_HPP
