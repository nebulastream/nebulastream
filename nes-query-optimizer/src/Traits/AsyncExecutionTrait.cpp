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

#include <Traits/AsyncExecutionTrait.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <typeinfo>
#include <unordered_map>
#include <utility>

#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>

namespace NES
{

AsyncExecutionTrait::AsyncExecutionTrait(
    std::string executorType,
    std::unordered_map<std::string, std::string> config,
    const size_t batchSize,
    const size_t maxConcurrency,
    const size_t channelCapacity,
    const bool preserveOrder)
    : executorType(std::move(executorType))
    , config(std::move(config))
    , batchSize(batchSize)
    , maxConcurrency(maxConcurrency)
    , channelCapacity(channelCapacity)
    , preserveOrder(preserveOrder)
{
}

const std::type_info& AsyncExecutionTrait::getType() const
{
    return typeid(AsyncExecutionTrait);
}

bool AsyncExecutionTrait::operator==(const AsyncExecutionTrait& other) const
{
    return executorType == other.executorType && config == other.config && batchSize == other.batchSize
        && maxConcurrency == other.maxConcurrency && channelCapacity == other.channelCapacity && preserveOrder == other.preserveOrder;
}

size_t AsyncExecutionTrait::hash() const
{
    /// The config map is left out: its iteration order is unspecified, and the executor type
    /// together with the tuning values already separates operators well enough.
    return folly::hash::hash_combine(executorType, batchSize, maxConcurrency, channelCapacity, preserveOrder);
}

std::string AsyncExecutionTrait::explain(ExplainVerbosity) const
{
    return fmt::format(
        "AsyncExecutionTrait: {} (batch: {}, concurrency: {}, ordered: {})", executorType, batchSize, maxConcurrency, preserveOrder);
}

std::string_view AsyncExecutionTrait::getName() const
{
    return NAME;
}

Reflected Reflector<AsyncExecutionTrait>::operator()(const AsyncExecutionTrait& trait, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedAsyncExecutionTrait{
            .executorType = trait.executorType,
            .config = trait.config,
            .batchSize = trait.batchSize,
            .maxConcurrency = trait.maxConcurrency,
            .channelCapacity = trait.channelCapacity,
            .preserveOrder = trait.preserveOrder});
}

AsyncExecutionTrait Unreflector<AsyncExecutionTrait>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [executorType, config, batchSize, maxConcurrency, channelCapacity, preserveOrder]
        = context.unreflect<detail::ReflectedAsyncExecutionTrait>(reflected);
    return AsyncExecutionTrait{
        std::move(executorType), std::move(config), batchSize, maxConcurrency, channelCapacity, preserveOrder};
}

}
