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

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] Schema<UnqualifiedUnboundField, Ordered> getOutputSchema() const;
    [[nodiscard]] AsyncDelayAsyncOperator withInferredArguments(const AsyncArgumentsWithConstants& binding) const;
    [[nodiscard]] AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(AsyncExecutionContext& context, SharedRuntimeValues values) const;

    [[nodiscard]] std::optional<size_t> maxConcurrency() const { return concurrencyLimit; }

    static AsyncOperator create(AsyncOperatorRegistryArguments arguments);
};

static_assert(AsyncOperatorConcept<AsyncDelayAsyncOperator>);
}
