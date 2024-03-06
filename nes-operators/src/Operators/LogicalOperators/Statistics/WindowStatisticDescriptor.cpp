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

#include <Operators/LogicalOperators/Statistics/WindowStatisticDescriptor.hpp>
#include <sstream>

namespace NES::Experimental::Statistics {

WindowStatisticDescriptor::WindowStatisticDescriptor(const std::string& logicalSourceName,
                                                     const std::string& fieldName,
                                                     const std::string& timestampField,
                                                     uint64_t depth,
                                                     uint64_t windowSize,
                                                     uint64_t slideFactor)
    : logicalSourceName(logicalSourceName), fieldName(fieldName), timestampField(timestampField), depth(depth),
      windowSize(windowSize), slideFactor(slideFactor) {}

std::string WindowStatisticDescriptor::toString() const {
    std::stringstream ss;
    ss << "Logical Source Name: " << logicalSourceName << "\n"
       << "Field Name: " << fieldName << "\n"
       << "TimestampField: " << timestampField << "\n"
       << "Depth: " << depth << "\n"
       << "Window Size: " << windowSize << "\n"
       << "Slide Factor: " << slideFactor << "\n";
    return ss.str();
}

const std::string& WindowStatisticDescriptor::getLogicalSourceName() const { return logicalSourceName; }

const std::string& WindowStatisticDescriptor::getFieldName() const { return fieldName; }

const std::string& WindowStatisticDescriptor::gettimestampField() const { return timestampField; }

uint64_t WindowStatisticDescriptor::getDepth() const { return depth; }

uint64_t WindowStatisticDescriptor::getWindowSize() const { return windowSize; }

uint64_t WindowStatisticDescriptor::getSlideFactor() const { return slideFactor; }
}// namespace NES::Experimental::Statistics