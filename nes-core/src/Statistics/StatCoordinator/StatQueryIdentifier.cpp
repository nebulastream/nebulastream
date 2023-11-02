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

#include "Statistics/StatCoordinator/StatQueryIdentifier.hpp"

namespace NES::Experimental::Statistics {

StatQueryIdentifier::StatQueryIdentifier(const std::string& logicalSourceName,
                                         const std::string& fieldName,
                                         const StatCollectorType statCollectorType)
    : logicalSourceName(logicalSourceName), fieldName(fieldName), statCollectorType(statCollectorType) {}

const std::string& StatQueryIdentifier::getLogicalSourceName() const { return logicalSourceName; }

const std::string& StatQueryIdentifier::getFieldName() const { return fieldName; }

const StatCollectorType& StatQueryIdentifier::getStatCollectorType() const { return statCollectorType; }

bool StatQueryIdentifier::operator==(const StatQueryIdentifier& statQueryIdentifier) const {
    return logicalSourceName == statQueryIdentifier.getLogicalSourceName() && fieldName == statQueryIdentifier.getFieldName()
        && statCollectorType == statQueryIdentifier.getStatCollectorType();
}

}// namespace NES::Experimental::Statistics