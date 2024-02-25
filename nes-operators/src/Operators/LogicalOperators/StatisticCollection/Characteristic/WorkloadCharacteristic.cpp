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
#include <Operators/LogicalOperators/StatisticCollection/Characteristic/WorkloadCharacteristic.hpp>

namespace NES::Statistic {

CharacteristicPtr WorkloadCharacteristic::create(MetricPtr type, QueryId queryId, OperatorId operatorId) {
    return std::make_shared<WorkloadCharacteristic>(WorkloadCharacteristic(type, queryId, operatorId));
}

QueryId WorkloadCharacteristic::getQueryId() const { return queryId; }

OperatorId WorkloadCharacteristic::getOperatorId() const { return operatorId; }

bool WorkloadCharacteristic::operator==(const Characteristic& rhs) const {
    if (this->Characteristic::operator==(rhs) && rhs.instanceOf<WorkloadCharacteristic>()) {
        auto rhsWorkloadCharacteristic = dynamic_cast<const WorkloadCharacteristic&>(rhs);
        return queryId == rhsWorkloadCharacteristic.queryId && operatorId == rhsWorkloadCharacteristic.operatorId;
    }
    return false;
}

size_t WorkloadCharacteristic::hash() const { return std::hash<QueryId>{}(queryId) ^ std::hash<OperatorId>{}(operatorId); }

std::string WorkloadCharacteristic::toString() const {
    return "{ QueryId: " + std::to_string(queryId) + " OperatorId: " + std::to_string(operatorId) + " }";
}

WorkloadCharacteristic::WorkloadCharacteristic(MetricPtr type, QueryId queryId, OperatorId operatorId)
    : Characteristic(type), queryId(queryId), operatorId(operatorId) {}

}// namespace NES::Statistic