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
#include <Util/ReflectionFwd.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

/// Debug aid: NES_INFO-logs (ts, slot, anomaly_score) for every tuple it sees, then passes
/// anomaly_score through unchanged. Place its call where the WHERE clause's predicate reads its
/// *return value* (e.g. `WHERE FT_LOG_SCORE(ts, slot, anomaly_score) > FLOAT32(0.5)`) rather than
/// alongside anomaly_score in a SELECT list -- an unused projected column can get optimized away,
/// silently dropping the log call, but a value the WHERE clause itself consumes cannot.
class FtLogScoreLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "FT_LOG_SCORE";

    FtLogScoreLogicalFunction(LogicalFunction ts, LogicalFunction slot, LogicalFunction score);
    explicit FtLogScoreLogicalFunction(const std::vector<LogicalFunction>& children);

    [[nodiscard]] bool operator==(const FtLogScoreLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] FtLogScoreLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] FtLogScoreLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    static LogicalFunctionRegistryReturnType createFT_LOG_SCORE(LogicalFunctionRegistryArguments arguments); /// NOLINT(readability-identifier-naming)

private:
    DataType dataType;
    LogicalFunction ts;
    LogicalFunction slot;
    LogicalFunction score;

    friend Reflector<FtLogScoreLogicalFunction>;
};

namespace detail
{
struct ReflectedFtLogScoreLogicalFunction
{
    std::vector<LogicalFunction> children;
};
}

template <>
struct Unreflector<FtLogScoreLogicalFunction>
{
    FtLogScoreLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<FtLogScoreLogicalFunction>
{
    Reflected operator()(const FtLogScoreLogicalFunction& function, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<FtLogScoreLogicalFunction>);

}

FMT_OSTREAM(NES::FtLogScoreLogicalFunction);
