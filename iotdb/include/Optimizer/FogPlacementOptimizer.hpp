/**\brief:
 *         This is the interface for base optimizer that needed to be implemented by any new query optimizer.
 *
 */

#ifndef IOTDB_FOGPLACEMENTOPTIMIZER_HPP
#define IOTDB_FOGPLACEMENTOPTIMIZER_HPP

#include <Optimizer/FogExecutionPlan.hpp>
#include <iostream>
#include <Topology/FogTopologyPlan.hpp>
#include <Topology/FogTopologyManager.hpp>

namespace iotdb {
    class FogPlacementOptimizer {
    private:

    public:
        FogPlacementOptimizer() {};

        /**
         * Returns an execution graph based on the input query and fog topology.
         * @param inputQuery
         * @param fogTopologyPlan
         * @return
         */
        virtual FogExecutionPlan prepareExecutionPlan(InputQuery inputQuery, FogTopologyPlanPtr fogTopologyPlan) = 0;

        /**
         * Factory method returning different kind of optimizer.
         * @param optimizerName
         * @return instance of type BaseOptimizer
         */
        static FogPlacementOptimizer *getOptimizer(std::string optimizerName);

    };

}

#endif //IOTDB_FOGPLACEMENTOPTIMIZER_HPP
