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

#include <string>
#include <utility>
#include <vector>

#include <Functions/LogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <LogicalFunctionUnreflectionRegistry.hpp>

namespace NES
{

class DynamicIdentityLogicalFunction
{
    LogicalFunction child;

public:
    static constexpr std::string_view NAME = "DYNAMIC_IDENTITY";

    explicit DynamicIdentityLogicalFunction(LogicalFunction child) : child(std::move(child)) { }

    [[nodiscard]] DataType getDataType() const { return child.getDataType(); }

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const { return {child}; }

    [[nodiscard]] std::string_view getType() const { return NAME; }

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const
    {
        return fmt::format("DYNAMIC_IDENTITY({})", child.explain(verbosity));
    }

    [[nodiscard]] DynamicIdentityLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const
    {
        PRECONDITION(children.size() == 1, "DYNAMIC_IDENTITY expects one argument");
        return DynamicIdentityLogicalFunction{children.front()};
    }

    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const
    {
        return DynamicIdentityLogicalFunction{child.withInferredDataType(schema)};
    }

    [[nodiscard]] bool operator==(const DynamicIdentityLogicalFunction& other) const { return child == other.child; }

    friend struct Reflector<DynamicIdentityLogicalFunction>;
};

template <>
struct Reflector<DynamicIdentityLogicalFunction>
{
    Reflected operator()(const DynamicIdentityLogicalFunction& function, const ReflectionContext& context) const
    {
        return context.reflect(function.child);
    }
};

template <>
struct Unreflector<DynamicIdentityLogicalFunction>
{
    DynamicIdentityLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const
    {
        return DynamicIdentityLogicalFunction{context.unreflect<LogicalFunction>(reflected)};
    }
};

LogicalFunction registerDynamicIdentity(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 1, "DYNAMIC_IDENTITY expects one argument");
    return DynamicIdentityLogicalFunction{arguments.children.front()};
}

}

extern "C" void nes_plugin_init()
{
    NES::LogicalFunctionRegistry::registerPlugin("DYNAMIC_IDENTITY", NES::registerDynamicIdentity);
    NES::LogicalFunctionUnreflectionRegistry::registerPlugin(
        "DYNAMIC_IDENTITY",
        [](NES::LogicalFunctionUnreflectionRegistryArguments arguments)
        { return arguments.context.unreflect<NES::DynamicIdentityLogicalFunction>(arguments.data); });
}
