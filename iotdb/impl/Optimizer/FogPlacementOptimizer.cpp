#include <Optimizer/FogPlacementOptimizer.hpp>
#include <iostream>
#include <Optimizer/impl/BottomUp.hpp>
#include <Optimizer/impl/TopDown.hpp>

namespace iotdb {

    FogPlacementOptimizer *FogPlacementOptimizer::getOptimizer(std::string optimizerName) {
        if (optimizerName == "BottomUp") {
            return new BottomUp();
        } else if (optimizerName == "TopDown") {
            return new TopDown();
        } else {
            throw "Unkown optimizer type : " + optimizerName;
        }
    }

}
