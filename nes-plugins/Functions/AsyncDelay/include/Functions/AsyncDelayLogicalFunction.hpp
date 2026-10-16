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
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/AsyncLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

/// `ASYNC_DELAY(value, millis[, batch_size])`: evaluates to `value` after waiting `millis` milliseconds without blocking
/// a thread. A stand-in for a slow external service. Asynchronous only; there is no Nautilus implementation.
///
/// It also stands in for a service that answers several rows with one request: with `batch_size`, one wait covers up to
/// that many rows. Its second value, read with `ASYNC_OUTPUT(..., 1)`, is the number of rows the wait covered.
class AsyncDelayLogicalFunction final : public AsyncLogicalFunction
{
public:
    static constexpr std::string_view NAME = "AsyncDelay";

    AsyncDelayLogicalFunction(LogicalFunction value, LogicalFunction millis, std::optional<LogicalFunction> batchSize);

    [[nodiscard]] bool operator==(const AsyncDelayLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] AsyncDelayLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    /// The batch size, which the implementation needs before the first row arrives.
    [[nodiscard]] std::string getAsyncConfiguration() const override;

    /// The value, and the number of rows the wait covered.
    [[nodiscard]] size_t getNumberOfAsyncOutputs() const override { return 2; }

    static LogicalFunctionRegistryReturnType createASYNC_DELAY(LogicalFunctionRegistryArguments arguments);

private:
    DataType dataType;
    LogicalFunction value;
    LogicalFunction millis;
    std::optional<LogicalFunction> batchSize;
};

namespace detail
{
struct ReflectedAsyncDelayLogicalFunction
{
    LogicalFunction value;
    LogicalFunction millis;
    std::optional<LogicalFunction> batchSize;
};
}

template <>
struct Unreflector<AsyncDelayLogicalFunction>
{
    AsyncDelayLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<AsyncDelayLogicalFunction>
{
    Reflected operator()(const AsyncDelayLogicalFunction& function, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<AsyncDelayLogicalFunction>);

}

FMT_OSTREAM(NES::AsyncDelayLogicalFunction);
