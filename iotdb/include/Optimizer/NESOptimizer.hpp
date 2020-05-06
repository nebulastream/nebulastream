
/**\brief:
 *         This class is responsible for producing the execution graph for the queries on nes topology. We can have
 *         different type of optimizers that can be provided as input the query and nes topology information and based
 *         on that a query execution graph will be computed. The output will be a graph where the edges contain the
 *         information about the operators to be executed and the nodes where the execution is to be done.
 */

#ifndef NESOPTIMIZER_HPP
#define NESOPTIMIZER_HPP

#include "../Topology/NESTopologyPlan.hpp"
#include "NESExecutionPlan.hpp"

namespace NES {

class NESOptimizer {

  public:
    NESExecutionPlanPtr prepareExecutionGraph(std::string strategy,
                                              InputQueryPtr inputQuery,
                                              NESTopologyPlanPtr nesTopologyPlan);
};
}// namespace NES

#endif//NESOPTIMIZER_HPP
