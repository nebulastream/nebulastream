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

#pragma once

#include <cstdint>
#include <memory>
#include <utility>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>

namespace NES
{

/// Gives the statistic store writer and reader physical operators access to the worker's statistic store.
class StatisticStoreOperatorHandler final : public OperatorHandler
{
public:
    explicit StatisticStoreOperatorHandler(std::shared_ptr<AbstractStatisticStore> statisticStore)
        : statisticStore(std::move(statisticStore))
    {
    }

    void start(PipelineExecutionContext&, uint32_t) override { }

    void stop(QueryTerminationType, PipelineExecutionContext&) override { }

    [[nodiscard]] const std::shared_ptr<AbstractStatisticStore>& getStatisticStore() const { return statisticStore; }

private:
    std::shared_ptr<AbstractStatisticStore> statisticStore;
};

}
