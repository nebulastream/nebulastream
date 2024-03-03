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

#include <API/QueryAPI.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticKey.hpp>
#include <Util/Logger/Logger.hpp>
#include <sstream>
#include <utility>

namespace NES::Statistic {

StatisticKey::StatisticKey(CharacteristicPtr  characteristic)
    : characteristic(std::move(characteristic)) {}

CharacteristicPtr StatisticKey::getCharacteristic() const { return characteristic; }

bool StatisticKey::operator==(const StatisticKey& rhs) const {
    return (*characteristic) == (*rhs.characteristic);
}

bool StatisticKey::operator!=(const StatisticKey& rhs) const { return !(rhs == *this); }

std::string StatisticKey::toString() const {
    std::ostringstream oss;
    oss << "Characteristic(" << characteristic->toString() << ")";
    return oss.str();
}

std::size_t StatisticKey::hash() const { return characteristic->hash(); }

}// namespace NES::Statistic