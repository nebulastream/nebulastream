#include <Optimizer/BaseOptimizer.hpp>
#include <iostream>
#include <Optimizer/impl/BottomUp.hpp>

namespace iotdb {

    BaseOptimizer *BaseOptimizer::getOptimizer(std::string optimizerName) {
        if (optimizerName == "BottomUp") {
            return new BottomUp();
        } else {
            throw "Unkown optimizer type : " + optimizerName;
        }
    }

}
