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

#include "Statistics/StatManager/StatCollectorIdentifier.hpp"

namespace NES {

namespace Experimental::Statistics {

StatCollectorIdentifier::StatCollectorIdentifier(const std::string& physicalSourceName,
                                                 const std::string& fieldName,
                                                 const StatCollectorType statCollectorType)
    : physicalSourceName(physicalSourceName), fieldName(fieldName), statCollectorType(statCollectorType) {}

bool StatCollectorIdentifier::operator==(const StatCollectorIdentifier& statCollectorIdentifier) const {
    return physicalSourceName == statCollectorIdentifier.getPhysicalSourceName()
        && fieldName == statCollectorIdentifier.getFieldName()
        && statCollectorType == statCollectorIdentifier.getStatCollectorType();
}

std::string StatCollectorIdentifier::getPhysicalSourceName() const {
    return physicalSourceName;
}

std::string StatCollectorIdentifier::getFieldName() const {
    return fieldName;
}

StatCollectorType StatCollectorIdentifier::getStatCollectorType() const {
    return statCollectorType;
}

void StatCollectorIdentifier::setPhysicalSourceName(std::string& fieldName) {
    this->fieldName = fieldName;
}

}
}
