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

#include <StatisticCollection/StatisticRegistry/StatisticInfo.hpp>

namespace NES::Statistic {
StatisticInfo::StatisticInfo(const TriggerConditionPtr triggerCondition,
                             const std::function<void(CharacteristicPtr)>&& callBack,
                             const QueryId& queryId)
    : triggerCondition(std::move(triggerCondition)), callBack(callBack), queryId(queryId) {}

[[maybe_unused]] TriggerConditionPtr StatisticInfo::getTriggerCondition() const { return triggerCondition; }

const std::function<void(CharacteristicPtr)>& StatisticInfo::getCallBack() const { return callBack; }

QueryId StatisticInfo::getQueryId() const { return queryId; }

void StatisticInfo::stoppedQuery() { setQueryId(INVALID_QUERY_ID); }

bool StatisticInfo::isRunning() const { return queryId == INVALID_QUERY_ID; }

std::string StatisticInfo::toString() const {
    std::ostringstream oss;
    oss << "TriggerCondition: " << triggerCondition->toString() << " "
        << "QueryId: " << queryId << " ";
        if (callBack != nullptr) {
            oss << "Callback: " << callBack.target_type().name() << std::endl;
        } else {
            oss << "Callback: nullptr" << std::endl;
        }
    return oss.str();
}

void StatisticInfo::setQueryId(const QueryId queryId) { this->queryId = queryId; }

bool StatisticInfo::operator==(const StatisticInfo& rhs) const {
    return triggerCondition == rhs.triggerCondition && queryId == rhs.queryId;
}

bool StatisticInfo::operator!=(const StatisticInfo& rhs) const { return !(rhs == *this); }
}// namespace NES::Statistic