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

#include <LoweringRules/LowerToPhysical/TableIndexLowering.hpp>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/CastToTypeLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/BTree/BTree.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/Record.hpp>
#include <Operators/Windows/JoinFunctionSplit.hpp>
#include <Runtime/MemoryUtils.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <StreamTableJoin/StreamTableJoinOperatorHandler.hpp>
#include <StreamTableJoin/StreamTableJoinPhysicalOperator.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <ErrorHandling.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace NES
{

namespace
{
/// Lowers `field` cast to `type` (a no-op cast is skipped), reading from a record of the join input described by `mapping`.
PhysicalFunction lowerKeyFunction(const Field& field, const DataType& type, const FieldMappingTrait& mapping)
{
    const FieldAccessLogicalFunction fieldAccess{field};
    if (field.getDataType() == type)
    {
        return QueryCompilation::FunctionProvider::lowerFunction(fieldAccess, mapping);
    }
    return QueryCompilation::FunctionProvider::lowerFunction(CastToTypeLogicalFunction{type, fieldAccess}, mapping);
}

}

LoweredEquiKeys
lowerEquiKeys(const std::vector<EquiJoinKey>& equiKeys, const FieldMappingTrait& streamMapping, const FieldMappingTrait& tableMapping)
{
    LoweredEquiKeys keys;
    for (uint64_t i = 0; i < equiKeys.size(); ++i)
    {
        const auto& [streamField, tableField] = equiKeys[i];
        const auto keyType = streamField.getDataType().join(tableField.getDataType());
        INVARIANT(keyType.has_value(), "Equi-join keys must have a common type");
        keys.keyFields.emplace_back(
            QualifiedIdentifier::create(Identifier::parse("streamtablejoin"), Identifier::parse("key" + std::to_string(i))), *keyType);
        keys.streamKeyFunctions.push_back(lowerKeyFunction(streamField, *keyType, streamMapping));
        keys.tableKeyFunctions.push_back(lowerKeyFunction(tableField, *keyType, tableMapping));
    }
    return keys;
}

StreamTableJoinIndex createTableIndex(
    const LoweredEquiKeys& keys,
    const Schema<QualifiedUnboundField, Ordered>& tableSchema,
    const uint64_t numberOfBuckets,
    const QueryExecutionConfiguration& conf)
{
    PRECONDITION(!keys.keyFields.empty(), "A table hash index needs at least one equi-join key");
    std::vector<QualifiedUnboundField> entryFields = keys.keyFields;
    const auto keyNames = keys.keyFields | std::views::transform([](const auto& field) { return field.getFullyQualifiedName(); })
        | std::ranges::to<std::vector<Record::RecordFieldIdentifier>>();
    for (const auto& field : tableSchema)
    {
        entryFields.push_back(field);
    }
    const auto [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(
        Schema<QualifiedUnboundField, Ordered>{entryFields}, keyNames, getOrderedFieldNames(tableSchema));

    uint64_t entrySize = sizeof(ChainedHashMapEntry);
    for (const auto& field : entryFields)
    {
        entrySize += field.getDataType().getSizeInBytesWithNull();
    }
    /// The entries of the index are stored in pooled buffers of the operator buffer size.
    if (conf.operatorBufferSize < entrySize)
    {
        throw QueryCompilerError(
            "The table index needs buffers of at least {} bytes for one entry, but the operator buffer size is {}. Increase "
            "operator_buffer_size.",
            entrySize,
            conf.operatorBufferSize);
    }
    return StreamTableJoinIndex{
        .hashMapConfig
        = ChainedHashMapConfig{.entrySize = entrySize, .numberOfBuckets = numberOfBuckets, .bloomFilterParams = std::nullopt, .fieldKeys = fieldKeys, .fieldValues = fieldValues, .hashFunction = std::make_shared<MurMur3HashFunction>()},
        .streamKeyFunctions = keys.streamKeyFunctions,
        .tableKeyFunctions = keys.tableKeyFunctions};
}

PendingRowsLayout createPendingRowsLayout(const Schema<QualifiedUnboundField, Ordered>& streamSchema)
{
    const QualifiedUnboundField timestampField{pendingTimestampField(), DataTypeProvider::provideDataType(DataType::Type::UINT64)};
    std::vector<QualifiedUnboundField> entryFields{timestampField};
    for (const auto& field : streamSchema)
    {
        entryFields.push_back(field);
    }
    return PendingRowsLayout{
        .entryLayout = std::make_shared<DefaultBTreeTupleLayout>(Schema<QualifiedUnboundField, Ordered>{entryFields}),
        .sortKeyLayout = std::make_shared<DefaultBTreeTupleLayout>(Schema<QualifiedUnboundField, Ordered>{std::vector{timestampField}})};
}

StreamTableJoinOperatorHandler::TableIndexFactory createHashMapIndexFactory(ChainedHashMapConfig config)
{
    return [config = std::move(config)](AbstractBufferProvider& bufferProvider)
    {
        /// The index has more buckets than fit into a pooled buffer, as a table is usually larger than a slice of a join.
        auto buffer = getBufferOfAtLeast(
            ChainedHashMap::calculateBufferSize(config.numberOfBuckets, config.bloomFilterMemAreaSize()),
            bufferProvider,
            "The buckets of a stream-table join index");
        ChainedHashMap::init(buffer, config);
        return buffer;
    };
}

StreamTableJoinOperatorHandler::TableIndexFactory createBTreeIndexFactory(const uint64_t entrySize)
{
    return [entrySize](AbstractBufferProvider& bufferProvider)
    {
        auto buffer = bufferProvider.getBufferBlocking();
        BTree::init(buffer, bufferProvider.getBufferSize(), entrySize);
        return buffer;
    };
}

}
