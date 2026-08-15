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
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Reflection.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

class CaseLogicalFunction final
{
public:
    CaseLogicalFunction() = default;
    CaseLogicalFunction(std::vector<LogicalFunction> whenConditions, std::vector<LogicalFunction> thenResults, LogicalFunction defaultResult);

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] CaseLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;
    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] bool operator==(const CaseLogicalFunction& rhs) const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    static constexpr std::string_view NAME = "Case";

private:
    std::vector<LogicalFunction> whenConditions;
    std::vector<LogicalFunction> thenResults;
    LogicalFunction defaultResult;
    DataType dataType;

    friend Reflector<CaseLogicalFunction>;
    friend Unreflector<CaseLogicalFunction>;
};

struct ReflectedCaseLogicalFunction
{
    std::vector<LogicalFunction> whenConditions;
    std::vector<LogicalFunction> thenResults;
    LogicalFunction defaultResult;
};

template <>
struct Reflector<CaseLogicalFunction>
{
    Reflected operator()(const CaseLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<CaseLogicalFunction>
{
    CaseLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<CaseLogicalFunction>);

}

FMT_OSTREAM(NES::CaseLogicalFunction);
