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
#include <cstdint>
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

/// `ASYNC_OUTPUT(call, index)`: the index-th value of an async call that produces several, e.g. one per OUTPUT field of
/// a model. The call itself evaluates to the value with index 0. Equal calls are evaluated once, so reading several
/// values of one call costs one call. It is lifted with its call and has no implementation of its own: the Async
/// source writes the value straight into the field.
class AsyncOutputLogicalFunction final : public AsyncLogicalFunction
{
public:
    static constexpr std::string_view NAME = "AsyncOutput";

    AsyncOutputLogicalFunction(LogicalFunction call, size_t index);

    [[nodiscard]] bool operator==(const AsyncOutputLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] AsyncOutputLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    [[nodiscard]] const LogicalFunction& getCall() const { return call; }

    [[nodiscard]] size_t getIndex() const { return index; }

    static LogicalFunctionRegistryReturnType createASYNC_OUTPUT(LogicalFunctionRegistryArguments arguments);

private:
    DataType dataType;
    LogicalFunction call;
    size_t index;
};

namespace detail
{
struct ReflectedAsyncOutputLogicalFunction
{
    LogicalFunction call;
    uint64_t index;
};
}

template <>
struct Unreflector<AsyncOutputLogicalFunction>
{
    AsyncOutputLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<AsyncOutputLogicalFunction>
{
    Reflected operator()(const AsyncOutputLogicalFunction& function, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<AsyncOutputLogicalFunction>);

}

FMT_OSTREAM(NES::AsyncOutputLogicalFunction);
