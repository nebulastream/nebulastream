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

#include <StatisticCollection/Statistic/Metric/Cardinality.hpp>

namespace NES::Statistic {

MetricPtr Cardinality::create(const std::string& fieldName) {
    return std::make_shared<Cardinality>(Cardinality(fieldName));
}

bool Cardinality::operator==(const Metric& rhs) const {
    if (rhs.instanceOf<Cardinality>()) {
        auto rhsCardinality = dynamic_cast<const Cardinality&>(rhs);
        return fieldName == rhsCardinality.fieldName;
    }
    return false;
}

const std::string& Cardinality::getFieldName() const { return fieldName; }

Cardinality::Cardinality(const std::string& fieldName) : fieldName(fieldName) {}

}// namespace NES::Statistic