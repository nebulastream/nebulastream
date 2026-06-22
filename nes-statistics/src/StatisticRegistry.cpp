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
#include <StatisticRegistry.hpp>

#include <optional>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <ConditionTrigger.hpp>
#include <Statistic.hpp>

namespace NES
{

std::optional<StatisticRegistry::Entry> StatisticRegistry::find(const Key& key) const
{
    const auto locked = registry.rlock();
    if (const auto it = locked->find(key); it != locked->end())
    {
        return it->second;
    }
    return {};
}

void StatisticRegistry::registerStatistic(
    const Key& key, const QueryId queryId, const Statistic::StatisticId statisticId, std::vector<ConditionTrigger> triggers)
{
    registry.wlock()->insert_or_assign(key, Entry{.queryId = queryId, .statisticId = statisticId, .triggers = std::move(triggers)});
}

bool StatisticRegistry::addTrigger(const Key& key, ConditionTrigger trigger)
{
    const auto locked = registry.wlock();
    if (const auto it = locked->find(key); it != locked->end())
    {
        it->second.triggers.push_back(std::move(trigger));
        return true;
    }
    return false;
}

bool StatisticRegistry::deregisterStatistic(const Key& key)
{
    return registry.wlock()->erase(key) != 0U;
}

size_t StatisticRegistry::getNumberOfEntries()
{
    return registry.rlock()->size();
}

}
