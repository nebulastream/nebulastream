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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Statistics/ReservoirSampleDescriptor.hpp>

namespace NES::Experimental::Statistics {

ReservoirSampleDescriptor::ReservoirSampleDescriptor(uint64_t width) : WindowStatisticDescriptor(), width(width) {}

bool ReservoirSampleDescriptor::operator==(WindowStatisticDescriptor& statisticsDescriptor) {
    auto rsDesc = dynamic_cast<ReservoirSampleDescriptor*>(&statisticsDescriptor);
    if (rsDesc != nullptr) {
        if (width == rsDesc->getWidth()) {
            return true;
        }
        return false;
    }
    return false;
}

void ReservoirSampleDescriptor::addStatisticFields(NES::SchemaPtr schema) {
    schema->addField("width", BasicType::UINT64);
}

uint64_t ReservoirSampleDescriptor::getWidth() const { return width; }

}