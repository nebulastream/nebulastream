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
#include <cstdint>
#include <functional>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/Strings.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableQueryPlan.pb.h>
#include <SerializableTrait.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
namespace
{
constexpr std::string_view OUTPUT_ORIGIN_IDS_TRAIT_NAME = "OutputOriginIds";
constexpr std::string_view INPUT_FORMAT_CONFIG_KEY = "input_format";
constexpr std::string_view SINK_NAME_CONFIG_KEY = "SinkName";

void appendEscapedString(std::ostringstream& signature, const std::string_view value)
{
    signature << '"' << escapeSpecialCharacters(value) << '"';
}

void appendDataType(std::ostringstream& signature, const SerializableDataType& dataType)
{
    signature << static_cast<int>(dataType.type());
}

void appendSchema(std::ostringstream& signature, const SerializableSchema& schema)
{
    signature << "schema[";
    for (const auto& field : schema.fields())
    {
        appendEscapedString(signature, field.name());
        signature << ':';
        appendDataType(signature, field.type());
        signature << ';';
    }
    signature << ']';
}

void appendFunction(std::ostringstream& signature, const SerializableFunction& function);
void appendVariant(std::ostringstream& signature, const SerializableVariantDescriptor& variant);

void appendFunctionList(std::ostringstream& signature, const FunctionList& functionList)
{
    signature << "functions[";
    for (const auto& function : functionList.functions())
    {
        appendFunction(signature, function);
        signature << ';';
    }
    signature << ']';
}

void appendAggregationFunction(std::ostringstream& signature, const SerializableAggregationFunction& function)
{
    signature << "aggregation(type=";
    appendEscapedString(signature, function.type());
    signature << ",on=";
    appendFunction(signature, function.on_field());
    signature << ",as=";
    appendFunction(signature, function.as_field());
    signature << ')';
}

void appendAggregationFunctionList(std::ostringstream& signature, const AggregationFunctionList& functions)
{
    signature << "aggregations[";
    for (const auto& function : functions.functions())
    {
        appendAggregationFunction(signature, function);
        signature << ';';
    }
    signature << ']';
}

void appendWindowInfos(std::ostringstream& signature, const WindowInfos& windowInfos)
{
    signature << "windowInfos(timeCharacteristic=";
    if (windowInfos.has_time_characteristic())
    {
        const auto& timeCharacteristic = windowInfos.time_characteristic();
        signature << "type=" << static_cast<int>(timeCharacteristic.type());
        signature << ",field=";
        appendEscapedString(signature, timeCharacteristic.field());
        signature << ",multiplier=" << timeCharacteristic.multiplier();
    }
    else
    {
        signature << "<none>";
    }
    signature << ",windowType=";
    switch (windowInfos.window_type_case())
    {
        case WindowInfos::kTumblingWindow:
            signature << "tumbling(size=" << windowInfos.tumbling_window().size() << ')';
            break;
        case WindowInfos::kSlidingWindow:
            signature << "sliding(size=" << windowInfos.sliding_window().size() << ",slide=" << windowInfos.sliding_window().slide() << ')';
            break;
        case WindowInfos::WINDOW_TYPE_NOT_SET:
            signature << "<none>";
            break;
    }
    signature << ')';
}

void appendProjectionList(std::ostringstream& signature, const ProjectionList& projections)
{
    signature << "projections[";
    for (const auto& projection : projections.projections())
    {
        signature << "projection(identifier=";
        if (projection.has_identifier())
        {
            appendEscapedString(signature, projection.identifier());
        }
        else
        {
            signature << "<none>";
        }
        signature << ",function=";
        appendFunction(signature, projection.function());
        signature << ");";
    }
    signature << ']';
}

void appendVariant(std::ostringstream& signature, const SerializableVariantDescriptor& variant)
{
    switch (variant.value_case())
    {
        case SerializableVariantDescriptor::kIntValue:
            signature << "i32(" << variant.int_value() << ')';
            break;
        case SerializableVariantDescriptor::kUintValue:
            signature << "u32(" << variant.uint_value() << ')';
            break;
        case SerializableVariantDescriptor::kLongValue:
            signature << "i64(" << variant.long_value() << ')';
            break;
        case SerializableVariantDescriptor::kUlongValue:
            signature << "u64(" << variant.ulong_value() << ')';
            break;
        case SerializableVariantDescriptor::kBoolValue:
            signature << "bool(" << variant.bool_value() << ')';
            break;
        case SerializableVariantDescriptor::kCharValue:
            signature << "char(" << variant.char_value() << ')';
            break;
        case SerializableVariantDescriptor::kFloatValue:
            signature << "f32(" << variant.float_value() << ')';
            break;
        case SerializableVariantDescriptor::kDoubleValue:
            signature << "f64(" << variant.double_value() << ')';
            break;
        case SerializableVariantDescriptor::kStringValue:
            signature << "string(";
            appendEscapedString(signature, variant.string_value());
            signature << ')';
            break;
        case SerializableVariantDescriptor::kEnumValue:
            signature << "enum(";
            appendEscapedString(signature, variant.enum_value().value());
            signature << ')';
            break;
        case SerializableVariantDescriptor::kFunctionList:
            appendFunctionList(signature, variant.function_list());
            break;
        case SerializableVariantDescriptor::kAggregationFunctionList:
            appendAggregationFunctionList(signature, variant.aggregation_function_list());
            break;
        case SerializableVariantDescriptor::kWindowInfos:
            appendWindowInfos(signature, variant.window_infos());
            break;
        case SerializableVariantDescriptor::kProjections:
            appendProjectionList(signature, variant.projections());
            break;
        case SerializableVariantDescriptor::kUlongs:
            signature << "ulongs[";
            for (const auto value : variant.ulongs().values())
            {
                signature << value << ';';
            }
            signature << ']';
            break;
        case SerializableVariantDescriptor::VALUE_NOT_SET:
            signature << "<unset>";
            break;
    }
}

void appendFunction(std::ostringstream& signature, const SerializableFunction& function)
{
    signature << "function(type=";
    appendEscapedString(signature, function.function_type());
    signature << ",dataType=";
    appendDataType(signature, function.data_type());
    signature << ",config={";

    std::vector<std::pair<std::string, const SerializableVariantDescriptor*>> configEntries;
    configEntries.reserve(function.config().size());
    for (const auto& [key, value] : function.config())
    {
        configEntries.emplace_back(key, &value);
    }
    std::ranges::sort(configEntries, [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
    for (const auto& [key, value] : configEntries)
    {
        appendEscapedString(signature, key);
        signature << '=';
        appendVariant(signature, *value);
        signature << ';';
    }

    signature << "},children=[";
    for (const auto& child : function.children())
    {
        appendFunction(signature, child);
        signature << ';';
    }
    signature << "])";
}

template <typename MapType, typename Predicate>
void appendSortedConfig(std::ostringstream& signature, const MapType& config, Predicate&& predicate)
{
    std::vector<std::pair<std::string, const SerializableVariantDescriptor*>> configEntries;
    configEntries.reserve(config.size());
    for (const auto& [key, value] : config)
    {
        if (std::invoke(predicate, key))
        {
            configEntries.emplace_back(key, &value);
        }
    }
    std::ranges::sort(configEntries, [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

    signature << '{';
    for (const auto& [key, value] : configEntries)
    {
        appendEscapedString(signature, key);
        signature << '=';
        appendVariant(signature, *value);
        signature << ';';
    }
    signature << '}';
}

void appendTraitSet(std::ostringstream& signature, const SerializableTraitSet& traitSet)
{
    std::vector<std::pair<std::string, const SerializableTrait*>> traitEntries;
    traitEntries.reserve(traitSet.traits().size());
    for (const auto& [name, trait] : traitSet.traits())
    {
        if (name != OUTPUT_ORIGIN_IDS_TRAIT_NAME)
        {
            traitEntries.emplace_back(name, &trait);
        }
    }
    std::ranges::sort(traitEntries, [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

    signature << "traits{";
    for (const auto& [name, trait] : traitEntries)
    {
        appendEscapedString(signature, name);
        signature << '=';
        appendSortedConfig(signature, trait->config(), [](const auto&) { return true; });
        signature << ';';
    }
    signature << '}';
}

std::optional<std::reference_wrapper<const SerializableVariantDescriptor>> findConfigValue(
    const google::protobuf::Map<std::string, SerializableVariantDescriptor>& config, const std::string_view key)
{
    if (const auto it = config.find(std::string(key)); it != config.end())
    {
        return std::cref(it->second);
    }
    return std::nullopt;
}

void appendSourceDescriptor(std::ostringstream& signature, const SerializableSourceDescriptor& descriptor)
{
    signature << "sourceDescriptor(type=";
    appendEscapedString(signature, descriptor.sourcetype());
    signature << ",schema=";
    appendSchema(signature, descriptor.sourceschema());
    signature << ",parser={type=";
    appendEscapedString(signature, descriptor.parserconfig().type());
    signature << ",tupleDelimiter=";
    appendEscapedString(signature, descriptor.parserconfig().tupledelimiter());
    signature << ",fieldDelimiter=";
    appendEscapedString(signature, descriptor.parserconfig().fielddelimiter());
    signature << "})";
}

void appendSinkDescriptor(std::ostringstream& signature, const SerializableSinkDescriptor& descriptor)
{
    signature << "sinkDescriptor(type=";
    appendEscapedString(signature, descriptor.sinktype());
    signature << ",schema=";
    appendSchema(signature, descriptor.sinkschema());
    signature << ",inputFormat=";
    if (const auto inputFormat = findConfigValue(descriptor.config(), INPUT_FORMAT_CONFIG_KEY))
    {
        appendVariant(signature, inputFormat->get());
    }
    else
    {
        signature << "<none>";
    }
    signature << ')';
}

void appendSerializedOperatorBody(std::ostringstream& signature, const SerializableOperator& serializedOperator)
{
    signature << ",traits=";
    appendTraitSet(signature, serializedOperator.trait_set());

    if (serializedOperator.has_source())
    {
        signature << ",kind=source,value=";
        appendSourceDescriptor(signature, serializedOperator.source().sourcedescriptor());
        return;
    }

    if (serializedOperator.has_sink())
    {
        signature << ",kind=sink,value=";
        if (serializedOperator.sink().has_sinkdescriptor())
        {
            appendSinkDescriptor(signature, serializedOperator.sink().sinkdescriptor());
        }
        else
        {
            signature << "<none>";
        }
        return;
    }

    if (serializedOperator.has_operator_())
    {
        signature << ",kind=operator,type=";
        appendEscapedString(signature, serializedOperator.operator_().operator_type());
        signature << ",outputSchema=";
        appendSchema(signature, serializedOperator.operator_().output_schema());
        signature << ",inputSchemas=[";
        for (const auto& inputSchema : serializedOperator.operator_().input_schemas())
        {
            appendSchema(signature, inputSchema);
            signature << ';';
        }
        signature << "],config=";
        appendSortedConfig(signature, serializedOperator.config(), [](const std::string& key) { return key != SINK_NAME_CONFIG_KEY; });
    }
}

void appendSerializedOperator(
    std::ostringstream& signature,
    const uint64_t operatorId,
    const std::unordered_map<uint64_t, const SerializableOperator*>& operatorsById,
    std::unordered_map<uint64_t, uint64_t>& operatorOrdinals,
    uint64_t& nextOperatorOrdinal)
{
    if (const auto ordinal = operatorOrdinals.find(operatorId); ordinal != operatorOrdinals.end())
    {
        signature << "{ref=n" << ordinal->second << '}';
        return;
    }

    const auto operatorIterator = operatorsById.find(operatorId);
    if (operatorIterator == operatorsById.end())
    {
        signature << "{missing=" << operatorId << '}';
        return;
    }

    const auto operatorOrdinal = nextOperatorOrdinal++;
    operatorOrdinals.emplace(operatorId, operatorOrdinal);

    const auto& serializedOperator = *operatorIterator->second;
    signature << "{n=" << operatorOrdinal;
    appendSerializedOperatorBody(signature, serializedOperator);
    signature << ",children=[";
    for (const auto childId : serializedOperator.children_ids())
    {
        appendSerializedOperator(signature, childId, operatorsById, operatorOrdinals, nextOperatorOrdinal);
        signature << ';';
    }
    signature << "]}";
}
}

std::string OptimizedLogicalPlanSignatureUtil::create(const LogicalPlan& optimizedPlan, const QueryExecutionConfiguration& configuration)
{
    const auto serializedPlan = QueryPlanSerializationUtil::serializeQueryPlan(optimizedPlan);

    std::unordered_map<uint64_t, const SerializableOperator*> operatorsById;
    operatorsById.reserve(serializedPlan.operators_size());
    for (const auto& serializedOperator : serializedPlan.operators())
    {
        operatorsById.emplace(serializedOperator.operator_id(), &serializedOperator);
    }

    std::ostringstream signature;
    signature << "optimized-logical-plan-canonical";
    signature << "|executionMode=" << static_cast<int>(configuration.executionMode.getValue());
    signature << "|operatorBufferSize=" << configuration.operatorBufferSize.getValue();
    signature << "|pageSize=" << configuration.pageSize.getValue();
    signature << "|recordsPerKey=" << configuration.numberOfRecordsPerKey.getValue();
    signature << "|maxBuckets=" << configuration.maxNumberOfBuckets.getValue();
    signature << "|roots=[";

    std::unordered_map<uint64_t, uint64_t> operatorOrdinals;
    uint64_t nextOperatorOrdinal = 0;
    for (const auto rootOperatorId : serializedPlan.rootoperatorids())
    {
        appendSerializedOperator(signature, rootOperatorId, operatorsById, operatorOrdinals, nextOperatorOrdinal);
        signature << ';';
    }
    signature << ']';
    return signature.str();
}
}
