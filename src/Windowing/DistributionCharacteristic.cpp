/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <API/AttributeField.hpp>
#include <Windowing/DistributionCharacteristic.hpp>

namespace NES::Windowing {

DistributionCharacteristic::DistributionCharacteristic(Type type) : type(type) {}

DistributionCharacteristicPtr DistributionCharacteristic::createCompleteWindowType() {
    return std::make_shared<DistributionCharacteristic>(Type::Complete);
}

DistributionCharacteristicPtr DistributionCharacteristic::createSlicingWindowType() {
    return std::make_shared<DistributionCharacteristic>(Type::Slicing);
}

DistributionCharacteristicPtr DistributionCharacteristic::createCombiningWindowType() {
    return std::make_shared<DistributionCharacteristic>(Type::Combining);
}

DistributionCharacteristic::Type DistributionCharacteristic::getType() { return type; }

std::string DistributionCharacteristic::toString() {
    if (type == Complete) {
        return "Complete";
    } else if (type == Slicing) {
        return "Slicing";
    } else if (type == Combining) {
        return "Combining";
    } else {
        return "";
    }
}

}// namespace NES::Windowing