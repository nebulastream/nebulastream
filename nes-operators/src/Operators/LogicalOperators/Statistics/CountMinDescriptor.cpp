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
#include <Operators/LogicalOperators/Statistics/CountMinDescriptor.hpp>
#include <StatisticFieldIdentifiers.hpp>
#include <sstream>

namespace NES::Experimental::Statistics {

CountMinDescriptor::CountMinDescriptor(const std::string& logicalSourceName,
                                       const std::string& fieldName,
                                       const std::string& timestampField,
                                       uint64_t depth,
                                       uint64_t windowSize,
                                       uint64_t slideFactor,
                                       uint64_t width)
    : WindowStatisticDescriptor(logicalSourceName, fieldName, timestampField, depth, windowSize, slideFactor), width(width) {}

std::string CountMinDescriptor::toString() const {
    std::stringstream ss;
    ss << "CountMinDescriptor: "
       << "\n"
       << WindowStatisticDescriptor::toString() << " Width: " << width << "\n";
    return ss.str();
}

bool CountMinDescriptor::operator==(WindowStatisticDescriptor& statisticDescriptor) {
    auto countMinDesc = dynamic_cast<CountMinDescriptor*>(&statisticDescriptor);
    if (countMinDesc != nullptr) {
        if (getLogicalSourceName() == countMinDesc->getLogicalSourceName() && getFieldName() == countMinDesc->getFieldName()
            && getDepth() == countMinDesc->getDepth() && getWindowSize() == countMinDesc->getWindowSize()
            && getSlideFactor() == countMinDesc->getSlideFactor() && width == countMinDesc->getWidth()) {
            return true;
        }
        return false;
    }
    return false;
}

void CountMinDescriptor::addStatisticFields(NES::SchemaPtr schema) { schema->addField(getLogicalSourceName() + "$" +  WIDTH, BasicType::UINT64); }

uint64_t CountMinDescriptor::getWidth() const { return width; }

}// namespace NES::Experimental::Statistics
