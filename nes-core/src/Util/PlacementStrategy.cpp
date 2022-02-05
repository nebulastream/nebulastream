/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Util/Logger.hpp>
#include <Util/PlacementStrategy.hpp>
#include <string>

namespace NES {

PlacementStrategy::Value PlacementStrategy::getFromString(const std::string placementStrategy) {
    if (placementStrategy == "BottomUp") {
        return PlacementStrategy::BottomUp;
    } else if (placementStrategy == "TopDown") {
        return PlacementStrategy::TopDown;
    } else if (placementStrategy == "IFCOP") {
        return PlacementStrategy::IFCOP;
    } else if (placementStrategy == "ILP") {
        return PlacementStrategy::ILP;
    } else {
        NES_THROW_RUNTIME_ERROR("PlacementStrategy not supported " + placementStrategy);
    }
}

std::string PlacementStrategy::toString(const Value placementStrategy) {
    switch (placementStrategy) {
        case PlacementStrategy::TopDown: return "TopDown";
        case PlacementStrategy::BottomUp: return "BottomUp";
        case PlacementStrategy::IFCOP: return "IFCOP";
        case PlacementStrategy::ILP: return "ILP";
    }
}

}// namespace NES
