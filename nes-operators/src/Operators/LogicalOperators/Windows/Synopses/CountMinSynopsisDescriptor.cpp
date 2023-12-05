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
#include <Operators/Expressions/ExpressionNode.hpp>
#include <Operators/LogicalOperators/Windows/Synopses/CountMinSynopsisDescriptor.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Experimental::Statistics {

CountMinSynopsisDescriptor::CountMinSynopsisDescriptor(ExpressionNodePtr fieldName,
                                                       const double error,
                                                       const double probability,
                                                       const time_t windowSize,
                                                       const time_t slideFactor)
    : WindowSynopsisDescriptor(fieldName, SynopsisType::CountMin, windowSize, slideFactor), error(error),
      probability(probability) {}

bool CountMinSynopsisDescriptor::operator==(const NES::Experimental::Statistics::CountMinSynopsisDescriptor& otherDesc) {
    if (fieldName == otherDesc.getFieldName() && error == otherDesc.getError() && probability == otherDesc.getProbability()
        && windowSize == otherDesc.getWindowSize() && slideFactor == otherDesc.getSlideFactor()) {
        return true;
    } else {
        return false;
    }
}

std::string CountMinSynopsisDescriptor::toString() const {
    std::stringstream ss;
    ss << "SynopsisOperator: ";
    ss << " Type=" << this->getTypeAsString();
    ss << " fieldName=" << this->getFieldName();
    ss << " error=" << this->getError();
    ss << " probability=" << this->getProbability();
    return ss.str();
}

bool CountMinSynopsisDescriptor::equal(const NES::NodePtr& rhs) const {
    if (rhs->instanceOf<CountMinSynopsisDescriptor>()) {
        auto countMinOperator = rhs->as<CountMinSynopsisDescriptor>();
        if (countMinOperator->getError() == this->getError() && countMinOperator->getProbability() == this->getProbability()
            && countMinOperator->getFieldName() == this->getFieldName()
            && countMinOperator->getWindowSize() == this->getWindowSize()
            && countMinOperator->getSlideFactor() == this->getSlideFactor()) {
            return true;
        }
    }
    return false;
}

double_t CountMinSynopsisDescriptor::getError() const { return error; }

double_t CountMinSynopsisDescriptor::getProbability() const { return probability; }

}// namespace NES::Experimental::Statistics