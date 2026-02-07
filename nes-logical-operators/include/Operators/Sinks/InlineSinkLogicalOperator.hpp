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
#include <unordered_map>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

/// InlineSinkLogicalOperator objects represent sinks in the logical query plan that are defined within a query as opposed to
/// sinks defined in separate create statements. The InlineSinkLogicalOperator objects contain all necessary configurations to
/// build a SinkLogicalOperator within the InlineSinkBindingPhase of the optimizer.
class InlineSinkLogicalOperator
{
public:
    explicit InlineSinkLogicalOperator(
        std::string sinkType,
        const Schema& schema,
        std::unordered_map<std::string, std::string> config,
        std::unordered_map<std::string, std::string> formatConfig);

    [[nodiscard]] bool operator==(const InlineSinkLogicalOperator& rhs) const;
    static void serialize(SerializableOperator&);

    [[nodiscard]] InlineSinkLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] InlineSinkLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] static std::string_view getName() noexcept;

    [[nodiscard]] InlineSinkLogicalOperator withInferredSchema(const std::vector<Schema>& inputSchemas) const;

    [[nodiscard]] std::string getSinkType() const;
    [[nodiscard]] std::unordered_map<std::string, std::string> getSinkConfig() const;
    [[nodiscard]] Schema getSchema() const;
    [[nodiscard]] std::unordered_map<std::string, std::string> getFormatConfig() const;

private:
    static constexpr std::string_view NAME = "InlineSink";

    std::vector<LogicalOperator> children;
    TraitSet traitSet;

    Schema schema;
    std::string sinkType;
    std::unordered_map<std::string, std::string> sinkConfig;
    std::unordered_map<std::string, std::string> formatConfig;
};

static_assert(LogicalOperatorConcept<InlineSinkLogicalOperator>);
}
