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
#include <Operators/LogicalOperators/StatisticCollection/Characteristic/DataCharacteristic.hpp>
#include <sstream>
#include <utility>
namespace NES::Statistic {

CharacteristicPtr DataCharacteristic::create(MetricPtr type,
                                             const std::string& logicalSourceName,
                                             const std::initializer_list<std::string>& physicalSourceNames) {
    return std::make_shared<DataCharacteristic>(DataCharacteristic(std::move(type), logicalSourceName, physicalSourceNames));
}

std::string DataCharacteristic::getLogicalSourceName() const { return logicalSourceName; }

std::vector<std::string> DataCharacteristic::getPhysicalSourceNames() const { return physicalSourceNames; }

bool DataCharacteristic::operator==(const Characteristic& rhs) const {
    if (this->Characteristic::operator==(rhs) && rhs.instanceOf<DataCharacteristic>()) {
        auto rhsDataCharacteristic = dynamic_cast<const DataCharacteristic&>(rhs);
        return logicalSourceName == rhsDataCharacteristic.logicalSourceName
            && physicalSourceNames == rhsDataCharacteristic.physicalSourceNames;
    }
    return false;
}

std::string DataCharacteristic::toString() const {
    std::ostringstream oss;
    oss << "{ LogicalSourceName: " << logicalSourceName << " "
        << "PhysicalSourceNames: ";
    for (const auto& physicalSourceName : physicalSourceNames) {
        oss << physicalSourceName << " ";
    }
    oss << "}";
    return oss.str();
}

size_t DataCharacteristic::hash() const {
    size_t hash = 0;
    hash ^= std::hash<std::string>{}(logicalSourceName);
    for (const auto& str : physicalSourceNames) {
        hash ^= std::hash<std::string>{}(str);
    }
    return hash;
}

DataCharacteristic::DataCharacteristic(MetricPtr type,
                                       std::string logicalSourceName,
                                       const std::initializer_list<std::string>& physicalSourceNames)
    : Characteristic(std::move(type)), logicalSourceName(std::move(logicalSourceName)),
      physicalSourceNames(physicalSourceNames) {}

}// namespace NES::Statistic