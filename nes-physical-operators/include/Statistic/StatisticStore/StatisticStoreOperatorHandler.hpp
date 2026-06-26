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
#include <Runtime/Execution/OperatorHandler.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>

namespace NES
{

/// Holds the shared StatisticStore that the StatisticStoreWriter/Reader physical operators access at runtime.
/// The store instance is injected at construction (by the lowering rules, which receive it via the
/// LoweringRuleRegistryArguments constructor-DI in the query compiler).
class StatisticStoreOperatorHandler final : public OperatorHandler
{
public:
    explicit StatisticStoreOperatorHandler(std::shared_ptr<AbstractStatisticStore> statisticStore);

    void start(PipelineExecutionContext&, uint32_t) override { }

    void stop(QueryTerminationType, PipelineExecutionContext&) override { }

    [[nodiscard]] std::shared_ptr<AbstractStatisticStore> getStatisticStore() const;

private:
    std::shared_ptr<AbstractStatisticStore> statisticStore;
};

}
