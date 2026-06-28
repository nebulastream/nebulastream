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

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

class UdbRecordingLogicalOperator
{
public:
    UdbRecordingLogicalOperator() = default;

    explicit UdbRecordingLogicalOperator(std::optional<std::string> traceName) : traceName(std::move(traceName)) { }

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] static std::string_view getName() noexcept;

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] UdbRecordingLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] UdbRecordingLogicalOperator withTraitSet(TraitSet ts) const;
    [[nodiscard]] TraitSet getTraitSet() const;
    [[nodiscard]] bool operator==(const UdbRecordingLogicalOperator& rhs) const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;
    [[nodiscard]] UdbRecordingLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

    [[nodiscard]] const std::optional<std::string>& getTraceName() const { return traceName; }

private:
    static constexpr std::string_view NAME = "UdbRecording";
    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    std::optional<std::string> traceName;
};

template <>
struct Reflector<UdbRecordingLogicalOperator>
{
    Reflected operator()(const UdbRecordingLogicalOperator& op) const;
};

template <>
struct Unreflector<UdbRecordingLogicalOperator>
{
    UdbRecordingLogicalOperator operator()(const Reflected& reflected) const;
};

static_assert(LogicalOperatorConcept<UdbRecordingLogicalOperator>);

}

namespace NES::detail
{
struct ReflectedUdbRecordingLogicalOperator
{
    std::optional<std::string> traceName;
};
}
