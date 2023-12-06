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

#include <Operators/LogicalOperators/Statistics/CountMinDescriptor.hpp>
#include <API/Schema.hpp>

namespace NES::Experimental::Statistics {

CountMinDescriptor::CountMinDescriptor(uint64_t depth, uint64_t width) : WindowStatisticDescriptor(), depth(depth), width(width) {}

bool CountMinDescriptor::operator==(WindowStatisticDescriptor& statisticDescriptor) {
    auto countMinDesc = dynamic_cast<CountMinDescriptor*>(&statisticDescriptor);
    if (countMinDesc != nullptr) {
        if (depth == countMinDesc->getDepth() && width == countMinDesc->getWidth()) {
            return true;
        }
        return false;
    }
    return false;
}

void CountMinDescriptor::addStatisticFields(NES::SchemaPtr schema) {
    schema->addField("depth", BasicType::UINT64);
    schema->addField("width", BasicType::UINT64);
}

double CountMinDescriptor::getDepth() const { return depth; }

double CountMinDescriptor::getWidth() const { return width; }

}
