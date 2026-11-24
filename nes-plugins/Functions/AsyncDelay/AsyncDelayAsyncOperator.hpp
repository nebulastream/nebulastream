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
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncError.hpp>
#include <Sources/AsyncExecutionContext.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Sources/PreparedAsyncCall.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <AsyncOperatorRegistry.hpp>

namespace NES
{
/// Native serializable configuration for ASYNC_DELAY(value, millis[, max_concurrency]).
struct AsyncDelayAsyncOperator
{
    DataType valueType{DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE};
    std::optional<size_t> concurrencyLimit;
    AsyncInputAccessors inputs{};
    std::optional<AsyncInputAccessor> valueInput{};
    std::optional<AsyncInputAccessor> millisInput{};
    std::optional<AsyncInputAccessor> concurrencyInput{};

    [[nodiscard]] const AsyncInputAccessors& getInputAccessors() const { return inputs; }

    [[nodiscard]] AsyncDelayAsyncOperator withInputAccessors(AsyncInputAccessors accessors) const
    {
        auto copy = *this;
        copy.inputs = std::move(accessors);
        copy.valueInput = copy.inputs.size() >= 1 ? std::optional{copy.inputs[0]} : std::nullopt;
        copy.millisInput = copy.inputs.size() >= 2 ? std::optional{copy.inputs[1]} : std::nullopt;
        copy.concurrencyInput = copy.inputs.size() >= 3 ? std::optional{copy.inputs[2]} : std::nullopt;
        return copy;
    }

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] Schema<UnqualifiedUnboundField, Ordered> getOutputSchema() const;
    [[nodiscard]] AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(AsyncExecutionContext& context, const AsyncRowHandle& row) const;

    [[nodiscard]] std::optional<size_t> maxConcurrency() const { return concurrencyLimit; }

    static AsyncOperator create(AsyncOperatorRegistryArguments arguments);
    static AsyncOperator create(AsyncOperatorRegistryArguments arguments, std::optional<size_t> concurrencyLimit);
};

template <>
struct Reflector<AsyncDelayAsyncOperator>
{
    Reflected operator()(const AsyncDelayAsyncOperator& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<AsyncDelayAsyncOperator>
{
    AsyncDelayAsyncOperator operator()(const Reflected& data, const ReflectionContext& context) const;
};

static_assert(AsyncOperatorConcept<AsyncDelayAsyncOperator>);
}
