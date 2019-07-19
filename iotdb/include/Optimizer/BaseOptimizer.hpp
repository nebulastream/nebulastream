/**\brief:
 *         This is the interface for base optimizer that needed to be implemented by any new query optimizer.
 *
 */

#ifndef IOTDB_BASEOPTIMIZER_HPP
#define IOTDB_BASEOPTIMIZER_HPP

#include <Optimizer/OptimizedExecutionGraph.hpp>
#include <iostream>
#include <Topology/FogTopologyPlan.hpp>
#include <Topology/FogTopologyManager.hpp>

namespace iotdb {
    class BaseOptimizer {
    public:

        BaseOptimizer() {};

        /**
         * Returns an execution graph based on the input query and fog topology.
         * @param inputQuery
         * @param fogTopologyPlan
         * @return
         */
//        ExecutionGraph prepareExecutionPlan(InputQuery inputQuery, FogTopologyPlanPtr fogTopologyPlan) {
//            throw "Unimplemented Placement strategy";
//        };

        virtual ExecutionGraph prepareExecutionPlan(InputQuery inputQuery, FogTopologyPlanPtr fogTopologyPlan) = 0;

        /**
         * Factory method returning different kind of optimizer.
         * @param optimizerName
         * @return instance of type BaseOptimizer
         */
        static BaseOptimizer *getOptimizer(std::string optimizerName);

    };

}

#endif //IOTDB_BASEOPTIMIZER_HPP
