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

#include <Operators/LogicalOperators/Windows/Synopses/DDSketchSynopsisDescriptor.hpp>
#include <sstream>

namespace NES::Experimental::Statistics {
DDSketchSynopsisDescriptor::DDSketchSynopsisDescriptor(ExpressionNodePtr fieldName,
                                                       const uint64_t width,
                                                       const double error,
                                                       const time_t windowSize,
                                                       const time_t slideFactor)
    : WindowSynopsisDescriptor(fieldName, SynopsisType::DDSketch, windowSize, slideFactor), width(width), error(error) {}

std::string DDSketchSynopsisDescriptor::toString() const {
    std::stringstream ss;
    ss << "SynopsisOperator: ";
    ss << " Type=" << this->getTypeAsString();
    ss << " onField=" << this->getFieldName();
    ss << " width=" << this->getWidth();
    ss << " error=" << this->getError();
    return ss.str();
}

bool DDSketchSynopsisDescriptor::equal(const NES::NodePtr& rhs) const {
    if (rhs->instanceOf<DDSketchSynopsisDescriptor>()) {
        auto ddSketchOperator = rhs->as<DDSketchSynopsisDescriptor>();
        if (ddSketchOperator->getFieldName() == this->getFieldName() && ddSketchOperator->getWidth() == this->getWidth()
            && ddSketchOperator->getError() == this->getError()) {
            return true;
        }
    }
    return false;
}

uint64_t DDSketchSynopsisDescriptor::getWidth() const { return width; }

double DDSketchSynopsisDescriptor::getError() const { return error; }

}// namespace NES::Experimental::Statistics
