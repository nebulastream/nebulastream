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

#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

/// A call to a user-defined scalar function: `udfName(arg0, arg1, ...)`.
///
/// Created name-only by the SQL parser (the built-in function registry misses the
/// name), then resolved by the UDFResolutionRule, which attaches the catalog
/// `UdfDescriptor` (signature + `.so` path + entry point). The resolved descriptor
/// travels with the function across coordinator→worker serialization, so the worker
/// never needs the catalog.
class UDFCallLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "UDFCall";

    /// Parse-time placeholder: only the name and arguments are known.
    UDFCallLogicalFunction(std::string udfName, std::vector<LogicalFunction> arguments);
    /// Resolved form carrying the catalog descriptor.
    UDFCallLogicalFunction(std::string udfName, UdfDescriptor descriptor, std::vector<LogicalFunction> arguments);

    [[nodiscard]] bool operator==(const UDFCallLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] UDFCallLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    [[nodiscard]] const std::string& getUdfName() const { return udfName; }

    [[nodiscard]] const std::optional<UdfDescriptor>& getDescriptor() const { return descriptor; }

private:
    std::string udfName;
    std::optional<UdfDescriptor> descriptor;
    std::vector<LogicalFunction> arguments;
    DataType dataType;

    friend Reflector<UDFCallLogicalFunction>;
};

namespace detail
{
struct ReflectedUDFCallLogicalFunction
{
    std::string udfName;
    std::optional<UdfDescriptor> descriptor;
    std::vector<LogicalFunction> arguments;
};
}

template <>
struct Unreflector<UDFCallLogicalFunction>
{
    UDFCallLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<UDFCallLogicalFunction>
{
    Reflected operator()(const UDFCallLogicalFunction& function) const;
};

static_assert(LogicalFunctionConcept<UDFCallLogicalFunction>);
}

FMT_OSTREAM(NES::UDFCallLogicalFunction);
