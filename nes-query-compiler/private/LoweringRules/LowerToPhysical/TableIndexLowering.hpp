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

#include <cstdint>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Operators/Windows/JoinFunctionSplit.hpp>
#include <Schema/Schema.hpp>
#include <StreamTableJoin/StreamTableJoinOperatorHandler.hpp>
#include <StreamTableJoin/StreamTableJoinPhysicalOperator.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace NES
{
/// The equi-join keys of a stream-table or ASOF join, cast to a type common to both sides so that equal values hash and compare equal.
struct LoweredEquiKeys
{
    /// One field per key, named `streamtablejoin.key<i>`, with the common type.
    std::vector<QualifiedUnboundField> keyFields;
    /// Evaluate to the key fields, on a record of the stream (left) or the table (right) input.
    std::vector<PhysicalFunction> streamKeyFunctions;
    std::vector<PhysicalFunction> tableKeyFunctions;
};

LoweredEquiKeys
lowerEquiKeys(const std::vector<EquiJoinKey>& equiKeys, const FieldMappingTrait& streamMapping, const FieldMappingTrait& tableMapping);

/// Hash index on the table side of a stream-table join: each entry holds the equi-join keys, then the table row. Expects at least one key.
/// The map never rehashes: more rows than buckets only lengthen the chains.
StreamTableJoinIndex createTableIndex(
    const LoweredEquiKeys& keys,
    const Schema<QualifiedUnboundField, Ordered>& tableSchema,
    uint64_t numberOfBuckets,
    const QueryExecutionConfiguration& conf);

/// Layouts of the pending stream rows: [timestamp, stream row].
PendingRowsLayout createPendingRowsLayout(const Schema<QualifiedUnboundField, Ordered>& streamSchema);

StreamTableJoinOperatorHandler::TableIndexFactory createHashMapIndexFactory(ChainedHashMapConfig config);
/// Entries of `entrySize` bytes, in nodes of the buffer provider's buffer size.
StreamTableJoinOperatorHandler::TableIndexFactory createBTreeIndexFactory(uint64_t entrySize);
}
