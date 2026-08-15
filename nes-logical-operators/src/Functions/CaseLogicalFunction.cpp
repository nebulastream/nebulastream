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

#include <Functions/CaseLogicalFunction.hpp>

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

CaseLogicalFunction::CaseLogicalFunction(std::vector<LogicalFunction> whenConditions, std::vector<LogicalFunction> thenResults, LogicalFunction defaultResult)
    : whenConditions(std::move(whenConditions)), thenResults(std::move(thenResults)), defaultResult(std::move(defaultResult))
{
    PRECONDITION(this->whenConditions.size() == this->thenResults.size(), "CASE requires same number of when conditions and then results");
}

DataType CaseLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction CaseLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    CaseLogicalFunction copy = *this;
    for (auto& cond : copy.whenConditions) {
        cond = cond.withInferredDataType(schema);
    }
    for (auto& res : copy.thenResults) {
        res = res.withInferredDataType(schema);
    }
    copy.defaultResult = copy.defaultResult.withInferredDataType(schema);

    auto newType = copy.defaultResult.getDataType();
    for (const auto& res : copy.thenResults) {
        if (auto t = newType.join(res.getDataType()); t.has_value()) {
            newType = t.value();
        } else {
            throw CannotInferStamp("Could not join data types in CASE function branches.");
        }
    }
    copy.dataType = std::move(newType);
    copy.dataType.nullable = std::ranges::any_of(copy.getChildren(), [](const auto& child) { return child.getDataType().nullable; });
    return copy;
}

std::vector<LogicalFunction> CaseLogicalFunction::getChildren() const
{
    std::vector<LogicalFunction> children;
    for (size_t i = 0; i < whenConditions.size(); ++i) {
        children.push_back(whenConditions[i]);
        children.push_back(thenResults[i]);
    }
    children.push_back(defaultResult);
    return children;
}

CaseLogicalFunction CaseLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() >= 3 && children.size() % 2 == 1, "CaseLogicalFunction requires an odd number of children >= 3, got {}", children.size());
    std::vector<LogicalFunction> whens;
    std::vector<LogicalFunction> thens;
    size_t pairCount = (children.size() - 1) / 2;
    for (size_t i = 0; i < pairCount; ++i) {
        whens.push_back(children[2 * i]);
        thens.push_back(children[2 * i + 1]);
    }
    auto copy = *this;
    copy.whenConditions = std::move(whens);
    copy.thenResults = std::move(thens);
    copy.defaultResult = children.back();

    auto newType = copy.defaultResult.getDataType();
    for (const auto& res : copy.thenResults) {
        if (auto t = newType.join(res.getDataType()); t.has_value()) {
            newType = t.value();
        }
    }
    copy.dataType = newType;
    return copy;
}

std::string_view CaseLogicalFunction::getType() const
{
    return NAME;
}

bool CaseLogicalFunction::operator==(const CaseLogicalFunction& rhs) const
{
    if (whenConditions.size() != rhs.whenConditions.size()) return false;
    for (size_t i = 0; i < whenConditions.size(); ++i) {
        if (whenConditions[i] != rhs.whenConditions[i] || thenResults[i] != rhs.thenResults[i]) return false;
    }
    return defaultResult == rhs.defaultResult;
}

std::string CaseLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    std::string out = "CASE ";
    for (size_t i = 0; i < whenConditions.size(); ++i) {
        out += fmt::format("WHEN {} THEN {} ", whenConditions[i].explain(verbosity), thenResults[i].explain(verbosity));
    }
    out += fmt::format("ELSE {} END", defaultResult.explain(verbosity));
    return out;
}

Reflected Reflector<CaseLogicalFunction>::operator()(const CaseLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedCaseLogicalFunction{.whenConditions = function.whenConditions, .thenResults = function.thenResults, .defaultResult = function.defaultResult});
}

CaseLogicalFunction Unreflector<CaseLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [whenConditions, thenResults, defaultResult] = context.unreflect<detail::ReflectedCaseLogicalFunction>(reflected);
    return CaseLogicalFunction{std::move(whenConditions), std::move(thenResults), std::move(defaultResult)};
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterCaseLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() < 3 || arguments.children.size() % 2 != 1)
    {
        throw CannotDeserialize("CaseFunction requires an odd number of children >= 3, but got {}", arguments.children.size());
    }
    std::vector<LogicalFunction> whens;
    std::vector<LogicalFunction> thens;
    size_t pairCount = (arguments.children.size() - 1) / 2;
    for (size_t i = 0; i < pairCount; ++i) {
        whens.push_back(arguments.children[2 * i]);
        thens.push_back(arguments.children[2 * i + 1]);
    }
    return CaseLogicalFunction(std::move(whens), std::move(thens), arguments.children.back());
}

}
