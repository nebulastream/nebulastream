#include <Optimizer/BaseOptimizer.hpp>
#include <iostream>
#include <Optimizer/impl/HLF.hpp>

namespace iotdb {

    BaseOptimizer *BaseOptimizer::getOptimizer(std::string optimizerName) {
        if (optimizerName == "HLF") {
            return new HLF();
        } else {
            throw "Unkown optimizer type : " + optimizerName;
        }
    }

}
