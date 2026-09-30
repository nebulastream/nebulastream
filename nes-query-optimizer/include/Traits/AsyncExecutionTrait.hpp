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

#include <cstddef>
#include <string>
#include <string_view>
#include <typeinfo>
#include <unordered_map>

#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>

namespace NES

{

/// Marks an operator whose work is a slow call to something outside the engine — a model
/// server, a remote service — and that must therefore not run on a worker thread.
///
/// An operator carrying this trait is not lowered to a physical operator at all. Instead
/// `AsyncOperatorSplitter` cuts the plan there: the upstream half ends in a handoff sink,
/// and the operator itself becomes the consumer source of a second plan, where it runs on
/// that source's own thread and may block.
///
/// Everything here is plain data, because the trait travels to the worker as part of the
/// serialized plan and ends up in the generated source descriptor.
struct AsyncExecutionTrait final
{
    static constexpr std::string_view NAME = "AsyncExecution";

    /// Key into AsyncExecutorRegistry — which implementation does the actual work.
    std::string executorType;
    /// The operator's own settings, opaque to the framework.
    std::unordered_map<std::string, std::string> config;
    /// Records handed to the executor in one call.
    size_t batchSize = 1;
    /// Calls in flight at the same time.
    size_t maxConcurrency = 1;
    /// Buffers the handoff channel accepts before the producer is throttled.
    size_t channelCapacity = 64;
    /// Hand results downstream in input order.
    bool preserveOrder = true;

    AsyncExecutionTrait(
        std::string executorType,
        std::unordered_map<std::string, std::string> config,
        size_t batchSize,
        size_t maxConcurrency,
        size_t channelCapacity,
        bool preserveOrder);

    [[nodiscard]] const std::type_info& getType() const;

    bool operator==(const AsyncExecutionTrait& other) const;

    [[nodiscard]] size_t hash() const;

    [[nodiscard]] std::string explain(ExplainVerbosity) const;

    [[nodiscard]] std::string_view getName() const;

    friend Reflector<AsyncExecutionTrait>;
};

template <>
struct Reflector<AsyncExecutionTrait>
{
    Reflected operator()(const AsyncExecutionTrait& trait, const ReflectionContext& context) const;
};

template <>
struct Unreflector<AsyncExecutionTrait>
{
    AsyncExecutionTrait operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(TraitConcept<AsyncExecutionTrait>);

}

namespace NES::detail
{
struct
    ReflectedAsyncExecutionTrait /// NOLINT(bugprone-exception-escape) defaulted members on a struct holding a map trip the check; no real escape
{
    std::string executorType;
    std::unordered_map<std::string, std::string> config;
    size_t batchSize;
    size_t maxConcurrency;
    size_t channelCapacity;
    bool preserveOrder;
};
}
