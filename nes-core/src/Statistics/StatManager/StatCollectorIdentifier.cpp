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

#include <Statistics/StatManager/StatCollectorIdentifier.hpp>

namespace NES::Experimental::Statistics {

StatCollectorIdentifier::StatCollectorIdentifier(const std::string& logicalSourceName,
                                                 std::vector<std::string>& physicalSourceNames,
                                                 const std::string& fieldName,
                                                 const StatCollectorType statCollectorType)
    : logicalSourceName(logicalSourceName), physicalSourceNames(physicalSourceNames), fieldName(fieldName),
      statCollectorType(statCollectorType) {}

bool StatCollectorIdentifier::operator==(StatCollectorIdentifier& statCollectorIdentifier) {
    return this->logicalSourceName == statCollectorIdentifier.getLogicalSourceName()
        && this->physicalSourceNames == statCollectorIdentifier.getPhysicalSourceNames()
        && this->fieldName == statCollectorIdentifier.getFieldName()
        && this->statCollectorType == statCollectorIdentifier.getStatCollectorType();
}

std::string& StatCollectorIdentifier::getLogicalSourceName() { return logicalSourceName; }

std::vector<std::string>& StatCollectorIdentifier::getPhysicalSourceNames() { return physicalSourceNames; }

std::string& StatCollectorIdentifier::getFieldName() { return fieldName; }

StatCollectorType& StatCollectorIdentifier::getStatCollectorType() { return statCollectorType; }

void StatCollectorIdentifier::setPhysicalSourceName(std::string& physicalSourceName) {
    this->physicalSourceNames.push_back(physicalSourceName);
}

}// namespace NES::Experimental::Statistics
