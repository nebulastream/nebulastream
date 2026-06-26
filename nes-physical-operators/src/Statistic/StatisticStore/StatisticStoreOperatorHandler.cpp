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

#include <Statistic/StatisticStore/StatisticStoreOperatorHandler.hpp>

#include <memory>
#include <utility>
#include <StatisticStore/AbstractStatisticStore.hpp>

namespace NES
{

StatisticStoreOperatorHandler::StatisticStoreOperatorHandler(std::shared_ptr<AbstractStatisticStore> statisticStore)
    : statisticStore(std::move(statisticStore))
{
}

std::shared_ptr<AbstractStatisticStore> StatisticStoreOperatorHandler::getStatisticStore() const
{
    return statisticStore;
}

}
