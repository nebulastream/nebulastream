/**
 * This is the interface for base optimizer that needed to be implemented by any new query optimizer
 */

#ifndef IOTDB_BASEOPTIMIZER_HPP
#define IOTDB_BASEOPTIMIZER_HPP

namespace iotdb {
    class BaseOptimizer {
    public:
        void doSomething();
    };

}

#endif //IOTDB_BASEOPTIMIZER_HPP
