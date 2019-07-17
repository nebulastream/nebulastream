#ifndef IOTDB_HLF_HPP
#define IOTDB_HLF_HPP

#include <Optimizer/BaseOptimizer.hpp>

namespace iotdb {
    class HLF : public BaseOptimizer {
    public:
        ExecutionGraph prepareExecutionPlan(InputQuery inputQuery, FogTopologyPlan fogTopologyPlan);
    };
}

#endif //IOTDB_HLF_HPP
