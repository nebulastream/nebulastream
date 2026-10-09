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

#include <Functions/LogicalFunction.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/Reflection.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{
/// Constructs a fixed-sized array of N child expressions.
/// The child expressions must be able to be joined into a singular type, which will be the element type of the array.
/// Reachable from SQL via the type-constructor syntax (ARRAY(arg0, arg1, ...)).
/// In SQL, the size of the array does not have to be defined explicitly.
/// Array-Constants can currently not be created implicitly and always require the ARRAY() call.
class ConstructArrayLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "ConstructArray";

    ConstructArrayLogicalFunction(std::vector<LogicalFunction> children);

    [[nodiscard]] bool operator==(const ConstructArrayLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] ConstructArrayLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] ConstructArrayLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    static LogicalFunctionRegistryReturnType createConstructArray(LogicalFunctionRegistryArguments arguments);

private:
    DataType dataType;
    std::vector<LogicalFunction> children;
};

namespace detail
{
struct ReflectedConstructArrayLogicalFunction
{
    std::vector<LogicalFunction> children;
};
}

template <>
struct Reflector<ConstructArrayLogicalFunction>
{
    Reflected operator()(const ConstructArrayLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<ConstructArrayLogicalFunction>
{
    ConstructArrayLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<ConstructArrayLogicalFunction>);
}

FMT_OSTREAM(NES::ConstructArrayLogicalFunction);
