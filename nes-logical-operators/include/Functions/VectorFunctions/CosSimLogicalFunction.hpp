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

#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{
/// Logical function representing the cosine similarity between two dense float32 vectors (e.g., embeddings).
/// Expects two VARSIZED inputs containing packed little-endian FLOAT32 elements and produces a FLOAT64 value in [-1.0, 1.0].
/// Output nullability is inferred as nullable if either input is nullable.
/// Limitation: because DataType has no dedicated vector type, type inference accepts any VARSIZED input (including text),
/// and binary byte layouts are interpreted directly as packed FLOAT32 values at execution time.
class CosSimLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "COSINE_SIMILARITY";

    CosSimLogicalFunction(LogicalFunction left, LogicalFunction right);


    [[nodiscard]] bool operator==(const CosSimLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] CosSimLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    /// NOLINTNEXTLINE(readability-identifier-naming)
    static LogicalFunctionRegistryReturnType createCOSINE_SIMILARITY(LogicalFunctionRegistryArguments arguments);

private:
    DataType dataType;
    LogicalFunction left, right;
    friend Reflector<CosSimLogicalFunction>;
};

template <>
struct Reflector<CosSimLogicalFunction>
{
    Reflected operator()(const CosSimLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<CosSimLogicalFunction>
{
    CosSimLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<CosSimLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedCosSimLogicalFunction
{
    LogicalFunction left;
    LogicalFunction right;
};
}

FMT_OSTREAM(NES::CosSimLogicalFunction);
