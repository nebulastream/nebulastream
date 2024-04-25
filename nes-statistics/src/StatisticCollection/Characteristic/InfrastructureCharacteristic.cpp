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
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <fmt/format.h>
#include <StatisticCollection/Characteristic/InfrastructureCharacteristic.hpp>

namespace NES::Statistic {

CharacteristicPtr InfrastructureStatistic::create(MetricPtr type, WorkerId nodeId) {
    return std::make_shared<InfrastructureStatistic>(InfrastructureStatistic(type, nodeId));
}

bool InfrastructureStatistic::operator==(const Characteristic& rhs) const {
    if (this->Characteristic::operator==(rhs) && rhs.instanceOf<InfrastructureStatistic>()) {
        auto rhsInfrastructureStatistic = dynamic_cast<const InfrastructureStatistic&>(rhs);
        return nodeId == rhsInfrastructureStatistic.nodeId;
    }
    return false;
}

size_t InfrastructureStatistic::hash() const { return std::hash<WorkerId>()(nodeId); }

std::string InfrastructureStatistic::toString() const { return fmt::format("{{ NodeId: {} }}", nodeId); }

WorkerId InfrastructureStatistic::getNodeId() const { return nodeId; }

InfrastructureStatistic::InfrastructureStatistic(const MetricPtr type, const WorkerId nodeId)
    : Characteristic(type), nodeId(nodeId) {}

}// namespace NES::Statistic