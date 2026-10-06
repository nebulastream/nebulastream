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

#include <cstring>
#include <memory>
#include <optional>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <gtest/gtest.h>
#include "CompiledSerdeTestHarness.hpp"
#include <InputFormatter.hpp>
#include <PipelineState.hpp>
#include <RawTupleBuffer.hpp>
#include <ScanPhysicalOperator.hpp>
#include <SequenceShredder.hpp>

namespace NES
{
TEST(ScanSerdeTest, restoredScanCompletesPendingTuple)
{
    Testing::SerdePipelineContext context(1);
    const auto schema = Schema<QualifiedUnboundField, Ordered>{QualifiedUnboundField{Identifier::parse("value"), DataType::Type::UINT32}};
    auto memoryProvider = LowerSchemaProvider::lowerSchema(1024, schema, MemoryLayoutType::ROW_LAYOUT);

    TupleBuffer serialized;
    {
        auto raw = context.buffers->getBufferBlocking();
        std::memcpy(raw.getAvailableMemoryArea().data(), "12,", 3);
        raw.setNumberOfTuples(3);
        raw.setSequenceNumber(SequenceNumber(1));
        SequenceShredder donor;
        ASSERT_TRUE(donor.findSpanningTupleWithoutDelimiter(StagedBuffer{RawTupleBuffer{raw}, 0, 0}).isInRange);

        auto empty = context.buffers->getBufferBlocking();
        empty.setNumberOfTuples(0);
        empty.setSequenceNumber(SequenceNumber(2));
        ASSERT_TRUE(donor.findSpanningTupleWithoutDelimiter(StagedBuffer{RawTupleBuffer{empty}, 0, 0}).isInRange);

        auto donorFormatter = std::make_shared<InputFormatter>(nullptr, memoryProvider);
        donorFormatter->restoreShredder(donor.snapshot());
        ScanPhysicalOperator donorScan(donorFormatter, {});
        PipelineStateBuilder state;
        donorScan.emit(state, context);
        serialized = state.finish(context.getBufferManager());
    }

    auto replacementFormatter = std::make_shared<InputFormatter>(nullptr, memoryProvider);
    ScanPhysicalOperator replacementScan(replacementFormatter, {});
    PipelineStateReader state(serialized);
    replacementScan.absorb(state, context);
    state.ensureConsumed();

    SequenceShredder resumed;
    resumed.restore(replacementFormatter->snapshotShredder());
    auto last = context.buffers->getBufferBlocking();
    std::memcpy(last.getAvailableMemoryArea().data(), "34\n", 3);
    last.setNumberOfTuples(3);
    last.setSequenceNumber(SequenceNumber(3));
    const auto result = resumed.findLeadingSpanningTupleWithDelimiter(StagedBuffer{RawTupleBuffer{last}, 2, 2});
    ASSERT_TRUE(result.isInRange);
    ASSERT_EQ(result.spanningBuffers.getSize(), 4);
    EXPECT_EQ(result.spanningBuffers.getSpanningBuffers()[1].getBufferView(), "12,");
    EXPECT_TRUE(result.spanningBuffers.getSpanningBuffers()[2].getBufferView().empty());
    EXPECT_EQ(result.spanningBuffers.getSpanningBuffers()[3].getLeadingBytes(), "34");
}
}
