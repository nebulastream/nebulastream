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

#include <Statistics/StatCollectorIdentifier.hpp>

namespace NES {

namespace Experimental::Statistics {

StatCollectorIdentifier::StatCollectorIdentifier(const std::string& logicalSourceName,
                                                 const std::string& physicalSourceName,
                                                 const std::string& fieldName,
                                                 const std::string& expression)
    : logicalSourceName(logicalSourceName), physicalSourceName(physicalSourceName),
      fieldName(fieldName), expression(expression) {}

bool StatCollectorIdentifier::operator==(const StatCollectorIdentifier& statCollectorIdentifier) const {
    return logicalSourceName == statCollectorIdentifier.getLogicalSourceName()
        && physicalSourceName == statCollectorIdentifier.getPhysicalSourceName()
        && fieldName == statCollectorIdentifier.getFieldName() && expression == statCollectorIdentifier.getExpression();
}

std::string StatCollectorIdentifier::getLogicalSourceName() const {
    return logicalSourceName;
}

std::string StatCollectorIdentifier::getPhysicalSourceName() const {
    return physicalSourceName;
}

std::string StatCollectorIdentifier::getFieldName() const {
    return fieldName;
}

std::string StatCollectorIdentifier::getExpression() const {
    return expression;
}

}
}
