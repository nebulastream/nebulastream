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

#include <Operators/LogicalOperators/Windows/Synopses/ReservoirSampleSynopsisDescriptor.hpp>
#include <sstream>

namespace NES::Experimental::Statistics {
ReservoirSampleSynopsisDescriptor::ReservoirSampleSynopsisDescriptor(ExpressionNodePtr fieldName,
                                                                     const uint64_t width,
                                                                     const time_t windowSize,
                                                                     const time_t slideFactor)
    : WindowSynopsisDescriptor(fieldName, SynopsisType::RSample, windowSize, slideFactor), width(width) {}

bool ReservoirSampleSynopsisDescriptor::operator==(
    const NES::Experimental::Statistics::ReservoirSampleSynopsisDescriptor& otherDesc) {
    if (fieldName == otherDesc.getFieldName() && width == otherDesc.getWidth() && windowSize == otherDesc.getWindowSize()
        && slideFactor == otherDesc.getSlideFactor()) {
        return true;
    } else {
        return false;
    }
}

std::string ReservoirSampleSynopsisDescriptor::toString() const {
    std::stringstream ss;
    ss << "SynopsisOperator: ";
    ss << " Type=" << this->getTypeAsString();
    ss << " fieldName=" << this->getFieldName();
    ss << " width=" << this->getWidth();
    return ss.str();
}

bool ReservoirSampleSynopsisDescriptor::equal(const NES::NodePtr& rhs) const {
    if (rhs->instanceOf<ReservoirSampleSynopsisDescriptor>()) {
        auto sampleOperator = rhs->as<ReservoirSampleSynopsisDescriptor>();
        if (sampleOperator->getFieldName() == this->fieldName && sampleOperator->getWidth() == this->width
            && sampleOperator->getWindowSize() == this->windowSize && sampleOperator->getSlideFactor() == this->slideFactor) {
            return true;
        }
    }
    return false;
}

uint64_t ReservoirSampleSynopsisDescriptor::getWidth() const { return width; }
}// namespace NES::Experimental::Statistics
