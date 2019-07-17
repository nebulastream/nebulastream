#include <Optimizer/BaseOptimizer.hpp>
#include <iostream>
#include <Optimizer/impl/HLF.hpp>

using namespace iotdb;

BaseOptimizer* BaseOptimizer::getOptimizer(std::string optimizerName) {
    if(optimizerName == "HLF") {
        return new HLF;
    } else {
        return new BaseOptimizer;
    }
}

