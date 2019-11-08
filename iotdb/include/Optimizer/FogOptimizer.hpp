
/**\brief:
 *         This class is responsible for producing the execution graph for the queries on fog topology. We can have
 *         different type of optimizers that can be provided as input the query and fog topology information and based
 *         on that a query execution graph will be computed. The output will be a graph where the edges contain the
 *         information about the operators to be executed and the nodes where the execution is to be done.
 */


#ifndef IOTDB_FOGOPTIMIZER_HPP
#define IOTDB_FOGOPTIMIZER_HPP

#include "FogExecutionPlan.hpp"

namespace iotdb {

class FogOptimizer {

 public:
  FogExecutionPlan prepareExecutionGraph(std::string strategy,
                                         InputQuery inputQuery,
                                         FogTopologyPlanPtr fogTopologyPlan);
};
}

#endif //IOTDB_FOGOPTIMIZER_HPP
