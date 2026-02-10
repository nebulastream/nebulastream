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
    return getOutputSchema() == rhs.getOutputSchema() && getInputSchemas() == rhs.getInputSchemas() && getTraitSet() == rhs.getTraitSet()
        && config == rhs.config;
}

/*
void StoreLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);

    for (const auto& inputSchema : getInputSchemas())
    {
        auto* schProto = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputSchema, schProto);
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    for (const auto& [key, value] : config)
    {
        (*serializableOperator.mutable_config())[key] = descriptorConfigTypeToProto(value);
    }

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}
*/

StoreLogicalOperator StoreLogicalOperator::withConfig(DescriptorConfig::Config validatedConfig) const
{
    auto copy = *this;
    copy.config = std::move(validatedConfig);
    return copy;
}

DescriptorConfig::Config StoreLogicalOperator::validateAndFormatConfig(std::unordered_map<std::string, std::string> configPairs)
{
    return DescriptorConfig::validateAndFormat<ConfigParameters>(std::move(configPairs), std::string(NAME));
}

}

namespace NES
{

Reflected Reflector<StoreLogicalOperator>::operator()(const StoreLogicalOperator& op) const
{
    return reflect(detail::ReflectedStoreLogicalOperator{.config = op.getConfig()});
}

StoreLogicalOperator Unreflector<StoreLogicalOperator>::operator()(const Reflected& reflected) const
{
    auto [config] = unreflect<detail::ReflectedStoreLogicalOperator>(reflected);
    return StoreLogicalOperator(std::move(config));
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterStoreLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        const auto op = unreflect<StoreLogicalOperator>(arguments.reflected);
        return op.withInferredSchema(arguments.inputSchemas);
    }
    PRECONDITION(false, "Operator is only built directly via parser or via reflection, not using the registry");
    std::unreachable();
}

}
