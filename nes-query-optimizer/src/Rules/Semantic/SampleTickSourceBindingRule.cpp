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

#include <Rules/Semantic/SampleTickSourceBindingRule.hpp>

#include <ranges>
#include <string>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SampleLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Semantic/AnonymousSourceBindingRule.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

const std::type_info& SampleTickSourceBindingRule::getType()
{
    return typeid(SampleTickSourceBindingRule);
}

std::string_view SampleTickSourceBindingRule::getName()
{
    return NAME;
}

std::set<std::type_index> SampleTickSourceBindingRule::dependsOn() const
{
    return {typeid(AnonymousSourceBindingRule), typeid(LogicalSourceExpansionRule)};
}

std::set<std::type_index> SampleTickSourceBindingRule::requiredBy() const
{
    return {};
}

bool SampleTickSourceBindingRule::operator==(const SampleTickSourceBindingRule& other) const
{
    return sourceCatalog == other.sourceCatalog;
}

LogicalOperator SampleTickSourceBindingRule::bindSampleTickSource(const LogicalOperator& current) const
{
    /// @Yannik: Here we are adding the second source
    std::vector<LogicalOperator> newChildren;
    for (const auto& child : current.getChildren())
    {
        newChildren.emplace_back(bindSampleTickSource(child));
    }

    if (const auto sampleOp = current.tryGetAs<SampleLogicalOperator>(); sampleOp.has_value() && newChildren.size() == 1)
    {
        /// find any SourceDescriptorLogicalOperator in the data-source subtree and reuse its host.
        /// This means that we always assume to be on the same node as the data source.
        auto resolvedDataSources = BFSRange(newChildren.at(0))
            | std::views::filter([](const LogicalOperator& op) { return op.tryGetAs<SourceDescriptorLogicalOperator>().has_value(); })
            | std::views::transform([](const LogicalOperator& op) { return op.getAs<SourceDescriptorLogicalOperator>(); });
        const auto firstDataSource = resolvedDataSources.begin();
        PRECONDITION(
            firstDataSource != resolvedDataSources.end(),
            "SampleTickSourceBindingRule requires the data-source subtree to already be resolved to at least one "
            "SourceDescriptorLogicalOperator; ensure it runs after AnonymousSourceBindingRule/LogicalSourceExpansionRule");
        const auto host = (*firstDataSource)->getSourceDescriptor().getHost();

        /// "slot_ts": drives SampleTriggerPhysicalOperator's EventTimeFunction, i.e. it is the watermark
        /// the tick source's whole purpose is to stand in for the data watermark
        const Schema<UnqualifiedUnboundField, Ordered> tickSchema{
            std::vector<UnqualifiedUnboundField>{{Identifier::parse("slot_ts"), DataType::Type::UINT64}}};
        /// max_runtime_ms/max_ticks are only set when the query explicitly requests a bound; otherwise the key is
        /// left out entirely and SampleTickSource's own config defaults (run indefinitely) apply.
        std::unordered_map<Identifier, std::string> sourceConfig{
            {Identifier::parse("slot_duration_ms"), std::to_string(sampleOp.value()->getSlotDurationMs())}};
        if (const auto maxRuntimeMs = sampleOp.value()->getMaxRuntimeMs(); maxRuntimeMs.has_value())
        {
            sourceConfig.emplace(Identifier::parse("max_runtime_ms"), std::to_string(maxRuntimeMs.value()));
        }
        if (const auto maxTicks = sampleOp.value()->getMaxTicks(); maxTicks.has_value())
        {
            sourceConfig.emplace(Identifier::parse("max_ticks"), std::to_string(maxTicks.value()));
        }
        const std::unordered_map<Identifier, std::string> parserConfig{{Identifier::parse("type"), "CSV"}};

        const auto descriptorOpt
            = sourceCatalog->getAnonymousSource(Identifier::parse("SampleTick"), tickSchema, host, parserConfig, sourceConfig);
        if (!descriptorOpt.has_value())
        {
            throw InvalidConfigParameter("Could not create the SAMPLE tick source descriptor because of invalid config parameters");
        }
        newChildren.push_back(SourceDescriptorLogicalOperator::create(descriptorOpt.value()));
    }

    return current.withChildrenUnsafe(newChildren);
}

LogicalPlan SampleTickSourceBindingRule::apply(const LogicalPlan& queryPlan) const
{
    std::vector<LogicalOperator> newRoots;
    for (const auto& root : queryPlan.getRootOperators())
    {
        newRoots.emplace_back(bindSampleTickSource(root));
    }
    return queryPlan.withRootOperators(newRoots);
}

}
