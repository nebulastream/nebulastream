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

#include <Operators/StoreLogicalOperator.hpp>

#include <algorithm>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

std::string StoreLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        std::stringstream cfg;
        Descriptor tmp{DescriptorConfig::Config(config)};
        cfg << &tmp;
        return fmt::format("STORE(opId: {}, config: {}, schema: {})", id, cfg.str(), getOutputSchema());
    }
    return std::string("STORE");
}

std::string_view StoreLogicalOperator::getName() const noexcept
{
    return NAME;
}

StoreLogicalOperator StoreLogicalOperator::withTraitSet(TraitSet ts) const
{
    auto copy = *this;
    copy.traitSet = std::move(ts);
    return copy;
}

TraitSet StoreLogicalOperator::getTraitSet() const
{
    return traitSet;
}

StoreLogicalOperator StoreLogicalOperator::withChildren(std::vector<LogicalOperator> newChildren) const
{
    auto copy = *this;
    copy.children = std::move(newChildren);
    return copy;
}

std::vector<LogicalOperator> StoreLogicalOperator::getChildren() const
{
    return children;
}

std::vector<Schema> StoreLogicalOperator::getInputSchemas() const
{
    INVARIANT(!children.empty(), "Store operator requires exactly one child");
    return children | std::ranges::views::transform([](const LogicalOperator& child) { return child.getOutputSchema(); })
        | std::ranges::to<std::vector>();
}

Schema StoreLogicalOperator::getOutputSchema() const
{
    INVARIANT(!children.empty(), "Store operator requires exactly one child");
    return children.front().getOutputSchema();
}

StoreLogicalOperator StoreLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    INVARIANT(!inputSchemas.empty(), "Store should have at least one input");
    const auto& first = inputSchemas.front();
    for (const auto& s : inputSchemas)
    {
        if (s != first)
        {
            throw CannotInferSchema("All input schemas must be equal for Store operator");
        }
    }
    return copy;
}

bool StoreLogicalOperator::operator==(const StoreLogicalOperator& rhs) const
{
    bool result = getTraitSet() == rhs.getTraitSet() && config == rhs.config;
    if (!children.empty() && !rhs.children.empty())
    {
        result &= getOutputSchema() == rhs.getOutputSchema();
        result &= getInputSchemas() == rhs.getInputSchemas();
    }
    return result;
}

StoreLogicalOperator StoreLogicalOperator::withConfig(DescriptorConfig::Config validatedConfig) const
{
    auto copy = *this;
    copy.config = std::move(validatedConfig);
    return copy;
}

DescriptorConfig::Config StoreLogicalOperator::validateAndFormatConfig(std::unordered_map<std::string, std::string> configPairs)
{
    auto cfg = DescriptorConfig::validateAndFormat<ConfigParameters>(std::move(configPairs), std::string(NAME));

    const auto compression = Descriptor{DescriptorConfig::Config(cfg)}.getFromConfig(ConfigParameters::COMPRESSION);
    const bool header = std::get<bool>(cfg.at(ConfigParameters::HEADER));
    const bool directIO = std::get<bool>(cfg.at(ConfigParameters::DIRECT_IO));
    const auto chunkMin = std::get<uint32_t>(cfg.at(ConfigParameters::CHUNK_MIN_BYTES));
    if (directIO)
    {
        if (chunkMin % 4096 != 0)
        {
            throw InvalidConfigParameter(fmt::format("For direct_io=true, chunk_min_bytes must be 4096-byte aligned, got {}", chunkMin));
        }
    }
    if (compression != Replay::BinaryStoreCompressionCodec::None)
    {
        if (!header)
        {
            throw InvalidConfigParameter("Compressed store files require header=true");
        }
        if (directIO)
        {
            throw InvalidConfigParameter("Compressed store files do not support direct_io=true");
        }
        if (chunkMin == 0)
        {
            throw InvalidConfigParameter("Compressed store files require chunk_min_bytes > 0");
        }
    }
    return cfg;
}

Reflected Reflector<StoreLogicalOperator>::operator()(const StoreLogicalOperator& op) const
{
    const Descriptor descriptor{DescriptorConfig::Config(op.getConfig())};
    return reflect(detail::ReflectedStoreLogicalOperator{.config = descriptor.getReflectedConfig()});
}

StoreLogicalOperator Unreflector<StoreLogicalOperator>::operator()(const Reflected& reflected) const
{
    auto [config] = unreflect<detail::ReflectedStoreLogicalOperator>(reflected);
    return StoreLogicalOperator{Descriptor::unreflectConfig(config)};
}

}

namespace NES
{

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterStoreLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<StoreLogicalOperator>(arguments.reflected);
    }

    PRECONDITION(false, "Operator is only build directly via parser or via reflection, not using the registry");
    std::unreachable();
}

}
