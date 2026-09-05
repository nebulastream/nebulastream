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

#include <Serialization/OptimizedLogicalPlanSignatureUtil.hpp>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Serialization/TraitReflection.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/FieldOrderingTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Reflection.hpp>
#include <rfl/Generic.hpp>
#include <rfl/json/read.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
constexpr std::string_view OUTPUT_ORIGIN_IDS_TRAIT_NAME = "OutputOriginIds";

class CanonicalWriter
{
public:
    void marker(const char value) { output.push_back(value); }

    void boolean(const bool value)
    {
        marker('b');
        marker(value ? '1' : '0');
    }

    void integer(const int64_t value)
    {
        marker('i');
        output += std::to_string(value);
        marker(';');
    }

    void unsignedInteger(const uint64_t value)
    {
        marker('u');
        output += std::to_string(value);
        marker(';');
    }

    void floatingPoint(const double value)
    {
        marker('d');
        const auto bits = std::bit_cast<uint64_t>(value);
        static constexpr char digits[] = "0123456789abcdef";
        for (int shift = 60; shift >= 0; shift -= 4)
        {
            marker(digits[(bits >> shift) & 0x0fU]);
        }
        marker(';');
    }

    void string(const std::string_view value)
    {
        marker('s');
        output += std::to_string(value.size());
        marker(':');
        output.append(value);
    }

    [[nodiscard]] std::string take() { return std::move(output); }

private:
    std::string output;
};

template <size_t Extent>
void appendIdentifier(CanonicalWriter& writer, const QualifiedIdentifierBase<Extent>& identifier)
{
    writer.marker('q');
    writer.unsignedInteger(identifier.size());
    for (const auto& part : identifier)
    {
        writer.string(part.asCanonicalString());
        writer.boolean(part.isCaseSensitive());
    }
}

void appendDataType(CanonicalWriter& writer, const DataType& dataType)
{
    writer.marker('t');
    writer.unsignedInteger(static_cast<uint64_t>(dataType.type));
    writer.boolean(dataType.nullable);
}

template <typename FieldType, OrderType IsOrdered>
void appendSchema(CanonicalWriter& writer, const Schema<FieldType, IsOrdered>& schema)
{
    std::vector<std::string> fields;
    fields.reserve(schema.size());
    for (const auto& field : schema)
    {
        CanonicalWriter fieldWriter;
        appendIdentifier(fieldWriter, field.getFullyQualifiedName());
        appendDataType(fieldWriter, field.getDataType());
        fields.emplace_back(fieldWriter.take());
    }
    if constexpr (!IsOrdered.ordered)
    {
        std::ranges::sort(fields);
    }

    writer.marker(IsOrdered.ordered ? 'O' : 'U');
    writer.unsignedInteger(fields.size());
    for (const auto& field : fields)
    {
        writer.string(field);
    }
}

std::optional<int64_t> getGenericInteger(const rfl::Generic& value)
{
    if (const auto* integer = std::get_if<int64_t>(&value.variant()))
    {
        return *integer;
    }
    return std::nullopt;
}

void appendGeneric(CanonicalWriter& writer, const rfl::Generic& value, const std::unordered_map<OperatorId, uint64_t>& operatorOrdinals);

void appendGenericObject(
    CanonicalWriter& writer, const rfl::Generic::Object& object, const std::unordered_map<OperatorId, uint64_t>& operatorOrdinals)
{
    std::vector<std::pair<std::string_view, const rfl::Generic*>> entries;
    entries.reserve(object.size());
    for (const auto& [key, entry] : object)
    {
        if (key != "operatorId")
        {
            entries.emplace_back(key, &entry);
        }
    }
    std::ranges::sort(entries, {}, &std::pair<std::string_view, const rfl::Generic*>::first);

    writer.marker('{');
    writer.unsignedInteger(entries.size());
    for (const auto& [key, entry] : entries)
    {
        writer.string(key);
        if (key == "producedBy")
        {
            if (const auto rawOperatorId = getGenericInteger(*entry); rawOperatorId.has_value() && *rawOperatorId >= 0)
            {
                if (const auto ordinal = operatorOrdinals.find(OperatorId{static_cast<uint64_t>(*rawOperatorId)});
                    ordinal != operatorOrdinals.end())
                {
                    writer.marker('r');
                    writer.unsignedInteger(ordinal->second);
                    continue;
                }
            }
        }
        appendGeneric(writer, *entry, operatorOrdinals);
    }
    writer.marker('}');
}

void appendGeneric(CanonicalWriter& writer, const rfl::Generic& value, const std::unordered_map<OperatorId, uint64_t>& operatorOrdinals)
{
    std::visit(
        [&](const auto& variant)
        {
            using VariantType = std::remove_cvref_t<decltype(variant)>;
            if constexpr (std::is_same_v<VariantType, bool>)
            {
                writer.boolean(variant);
            }
            else if constexpr (std::is_same_v<VariantType, int64_t>)
            {
                writer.integer(variant);
            }
            else if constexpr (std::is_same_v<VariantType, double>)
            {
                writer.floatingPoint(variant);
            }
            else if constexpr (std::is_same_v<VariantType, std::string>)
            {
                writer.string(variant);
            }
            else if constexpr (std::is_same_v<VariantType, rfl::Generic::Object>)
            {
                appendGenericObject(writer, variant, operatorOrdinals);
            }
            else if constexpr (std::is_same_v<VariantType, rfl::Generic::Array>)
            {
                writer.marker('[');
                writer.unsignedInteger(variant.size());
                for (const auto& entry : variant)
                {
                    appendGeneric(writer, entry, operatorOrdinals);
                }
                writer.marker(']');
            }
            else if constexpr (std::is_same_v<VariantType, std::nullopt_t>)
            {
                writer.marker('n');
            }
        },
        value.variant());
}

void appendUnboundField(CanonicalWriter& writer, const UnqualifiedUnboundField& field)
{
    appendIdentifier(writer, field.getFullyQualifiedName());
    appendDataType(writer, field.getDataType());
}

void appendTraitSet(CanonicalWriter& writer, const TraitSet& traitSet, const std::unordered_map<OperatorId, uint64_t>& operatorOrdinals)
{
    std::vector<std::pair<std::string, std::string>> traits;
    traits.reserve(traitSet.size());
    const ReflectionContext context;
    for (const auto& trait : traitSet)
    {
        const auto name = std::string(trait.getName());
        if (name == OUTPUT_ORIGIN_IDS_TRAIT_NAME)
        {
            continue;
        }

        CanonicalWriter traitWriter;
        if (const auto fieldMapping = trait.tryGetAs<FieldMappingTrait>())
        {
            std::vector<std::string> mappings;
            mappings.reserve(fieldMapping.value()->getUnderlying().size());
            for (const auto& [source, target] : fieldMapping.value()->getUnderlying())
            {
                CanonicalWriter mappingWriter;
                appendUnboundField(mappingWriter, source);
                appendIdentifier(mappingWriter, target);
                mappings.emplace_back(mappingWriter.take());
            }
            std::ranges::sort(mappings);
            traitWriter.unsignedInteger(mappings.size());
            for (const auto& mapping : mappings)
            {
                traitWriter.string(mapping);
            }
        }
        else if (const auto fieldOrdering = trait.tryGetAs<FieldOrderingTrait>())
        {
            appendSchema(traitWriter, fieldOrdering.value()->getOrderedFields());
        }
        else
        {
            appendGeneric(traitWriter, *context.reflect(trait), operatorOrdinals);
        }
        traits.emplace_back(name, traitWriter.take());
    }
    std::ranges::sort(traits);

    writer.marker('T');
    writer.unsignedInteger(traits.size());
    for (const auto& [name, config] : traits)
    {
        writer.string(name);
        writer.string(config);
    }
}

void appendSourceDescriptor(
    CanonicalWriter& writer, const SourceDescriptor& descriptor, const std::unordered_map<OperatorId, uint64_t>& operatorOrdinals)
{
    const ReflectionContext context;
    writer.marker('S');
    writer.string(descriptor.getSourceType());
    writer.string(descriptor.getInputFormatType());
    writer.boolean(descriptor.isAnonymousSource());
    appendSchema(writer, *descriptor.getLogicalSource().getSchema());
    appendGeneric(writer, *context.reflect(descriptor.getInputFormatterDescriptor()), operatorOrdinals);
}

void appendSinkSchema(CanonicalWriter& writer, const SinkDescriptor& descriptor)
{
    std::visit(
        [&](const auto& schema)
        {
            using SchemaType = std::remove_cvref_t<decltype(schema)>;
            if constexpr (std::is_same_v<SchemaType, std::monostate>)
            {
                writer.marker('n');
            }
            else
            {
                PRECONDITION(schema != nullptr, "Sink descriptor contains a null schema");
                appendSchema(writer, *schema);
            }
        },
        descriptor.getSchema());
}

void appendSinkDescriptor(CanonicalWriter& writer, const SinkDescriptor& descriptor)
{
    writer.marker('K');
    writer.string(descriptor.getSinkType());
    writer.string(descriptor.getFormatType());
    writer.boolean(descriptor.isAnonymous());
    appendSinkSchema(writer, descriptor);

    std::vector<std::pair<std::string, std::string>> formatterConfig;
    for (const auto& [key, value] : descriptor.getOutputFormatterConfig())
    {
        formatterConfig.emplace_back(key.asCanonicalString(), value);
    }
    std::ranges::sort(formatterConfig);
    writer.unsignedInteger(formatterConfig.size());
    for (const auto& [key, value] : formatterConfig)
    {
        writer.string(key);
        writer.string(value);
    }
}

void assignOperatorOrdinals(
    const LogicalOperator& logicalOperator, std::unordered_map<OperatorId, uint64_t>& operatorOrdinals, uint64_t& nextOrdinal)
{
    if (!operatorOrdinals.emplace(logicalOperator.getId(), nextOrdinal).second)
    {
        return;
    }
    ++nextOrdinal;
    for (const auto& child : logicalOperator.getChildren())
    {
        assignOperatorOrdinals(child, operatorOrdinals, nextOrdinal);
    }
}

std::unordered_map<OperatorId, ReflectedOperator> reflectOperators(const LogicalPlan& optimizedPlan)
{
    const auto serializedPlan = QueryPlanSerializationUtil::serializeQueryPlan(optimizedPlan);
    std::unordered_map<OperatorId, ReflectedOperator> reflectedOperators;
    reflectedOperators.reserve(serializedPlan.reflectedoperators_size());
    const ReflectionContext context;
    for (const auto& serializedOperator : serializedPlan.reflectedoperators())
    {
        const auto generic = rfl::json::read<rfl::Generic>(serializedOperator);
        PRECONDITION(generic.has_value(), "Failed to parse a reflected operator while computing a plan signature");
        auto reflectedOperator = context.unreflect<ReflectedOperator>(Reflected{*generic});
        const auto operatorId = reflectedOperator.operatorId;
        PRECONDITION(
            reflectedOperators.emplace(operatorId, std::move(reflectedOperator)).second,
            "Duplicate operator id {} while computing a plan signature",
            operatorId.getRawValue());
    }
    return reflectedOperators;
}

void appendOperator(
    CanonicalWriter& writer,
    const LogicalOperator& logicalOperator,
    const std::unordered_map<OperatorId, ReflectedOperator>& reflectedOperators,
    const std::unordered_map<OperatorId, uint64_t>& operatorOrdinals,
    std::unordered_set<OperatorId>& emittedOperators)
{
    const auto ordinal = operatorOrdinals.at(logicalOperator.getId());
    if (!emittedOperators.emplace(logicalOperator.getId()).second)
    {
        writer.marker('R');
        writer.unsignedInteger(ordinal);
        return;
    }

    const auto reflectedOperator = reflectedOperators.find(logicalOperator.getId());
    PRECONDITION(reflectedOperator != reflectedOperators.end(), "Logical operator is missing reflected configuration");

    writer.marker('N');
    writer.unsignedInteger(ordinal);
    writer.string(logicalOperator.getName());
    const auto sink = logicalOperator.tryGetAs<SinkLogicalOperator>();
    if (sink.has_value())
    {
        writer.marker('n');
    }
    else
    {
        appendSchema(writer, logicalOperator.getOutputSchema());
    }
    appendTraitSet(writer, logicalOperator.getTraitSet(), operatorOrdinals);

    if (const auto source = logicalOperator.tryGetAs<SourceDescriptorLogicalOperator>())
    {
        appendSourceDescriptor(writer, source.value()->getSourceDescriptor(), operatorOrdinals);
    }
    else if (sink.has_value())
    {
        if (const auto descriptor = sink.value()->getSinkDescriptor())
        {
            appendSinkDescriptor(writer, *descriptor);
        }
        else
        {
            writer.marker('n');
        }
    }
    else
    {
        appendGeneric(writer, *reflectedOperator->second.config, operatorOrdinals);
    }

    const auto children = logicalOperator.getChildren();
    writer.marker('C');
    writer.unsignedInteger(children.size());
    for (const auto& child : children)
    {
        appendOperator(writer, child, reflectedOperators, operatorOrdinals, emittedOperators);
    }
}

void appendConfiguration(CanonicalWriter& writer, const QueryExecutionConfiguration& configuration)
{
    writer.marker('Q');
    writer.unsignedInteger(static_cast<uint64_t>(configuration.executionMode.getValue()));
    writer.unsignedInteger(configuration.operatorBufferSize.getValue());
    writer.unsignedInteger(configuration.pageSize.getValue());
    writer.unsignedInteger(configuration.numberOfRecordsPerKey.getValue());
    writer.unsignedInteger(configuration.numberOfPartitions.getValue());
    writer.boolean(configuration.bloomFilterConfiguration.enableBloomFilter.getValue());
    writer.floatingPoint(configuration.bloomFilterConfiguration.falsePositiveRate.getValue());
    writer.unsignedInteger(configuration.bloomFilterConfiguration.expectedEntries.getValue());
    writer.boolean(configuration.sliceCacheConfiguration.enableSliceCache.getValue());
    writer.unsignedInteger(configuration.sliceCacheConfiguration.numberOfEntries.getValue());
}
}

std::string OptimizedLogicalPlanSignatureUtil::create(const LogicalPlan& optimizedPlan, const QueryExecutionConfiguration& configuration)
{
    CanonicalWriter writer;
    writer.string("nes.optimized-logical-plan.v3");
    appendConfiguration(writer, configuration);

    std::unordered_map<OperatorId, uint64_t> operatorOrdinals;
    uint64_t nextOrdinal = 0;
    for (const auto& root : optimizedPlan.getRootOperators())
    {
        assignOperatorOrdinals(root, operatorOrdinals, nextOrdinal);
    }

    const auto reflectedOperators = reflectOperators(optimizedPlan);
    std::unordered_set<OperatorId> emittedOperators;
    writer.marker('G');
    writer.unsignedInteger(optimizedPlan.getRootOperators().size());
    for (const auto& root : optimizedPlan.getRootOperators())
    {
        appendOperator(writer, root, reflectedOperators, operatorOrdinals, emittedOperators);
    }
    return writer.take();
}
}
