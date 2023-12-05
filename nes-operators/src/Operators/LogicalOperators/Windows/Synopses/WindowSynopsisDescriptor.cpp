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

#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Windows/Synopses/WindowSynopsisDescriptor.hpp>
#include <sstream>

namespace NES::Experimental::Statistics {

WindowSynopsisDescriptor::WindowSynopsisDescriptor(ExpressionNodePtr fieldName,
                                                   const WindowSynopsisDescriptor::SynopsisType synopsisType,
                                                   const time_t windowSize,
                                                   const time_t slideFactor)
    : fieldName(fieldName), synopsisType(synopsisType), windowSize(windowSize),
      slideFactor(slideFactor) {}

std::string WindowSynopsisDescriptor::toString() const {
    std::stringstream ss;
    ss << "WindowSynopsis: ";
    ss << " Type: " << getTypeAsString();
    ss << " onField: " << this->getFieldName();
    ss << " windowSize: " << this->getWindowSize();
    ss << " slideFactor: " << this->getSlideFactor();
    ss << std::endl;
    return ss.str();
}

WindowSynopsisDescriptor::SynopsisType WindowSynopsisDescriptor::getType() { return synopsisType; }

std::string WindowSynopsisDescriptor::getTypeAsString() const {
    switch (synopsisType) {
        case SynopsisType::CountMin: return "CountMin";
        case SynopsisType::HLL: return "HyperLogLog";
        case SynopsisType::DDSketch: return "DDSketch";
        case SynopsisType::RSample: return "Reservoir Sample";
        default: return "Unknown Synopsis Type";
    }
}

ExpressionNodePtr WindowSynopsisDescriptor::getFieldName() const { return fieldName; }

time_t WindowSynopsisDescriptor::getWindowSize() const { return windowSize; }

time_t WindowSynopsisDescriptor::getSlideFactor() const { return slideFactor; }
}// namespace NES::Experimental::Statistics
