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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/VectorFunctions/CosSimPhysicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <Arena.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
constexpr uint32_t POOLED_BUFFER_SIZE = 4096;
constexpr uint32_t NUMBER_OF_POOLED_BUFFERS = 64;
constexpr BufferAlignment BUFFER_ALIGNMENT{64};
constexpr double UNPOOLED_MEMORY_FRACTION = 0.5;
constexpr size_t TOTAL_MEMORY_IN_BYTES = static_cast<size_t>(NUMBER_OF_POOLED_BUFFERS) * POOLED_BUFFER_SIZE;
}

class CosSimPhysicalFunctionTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("CosSimPhysicalFunctionTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup CosSimPhysicalFunctionTest test class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        bufferManager = BufferManager::create(
            TOTAL_MEMORY_IN_BYTES,
            UNPOOLED_MEMORY_FRACTION,
            BUFFER_ALIGNMENT,
            POOLED_BUFFER_SIZE,
            std::make_shared<NesDefaultMemoryAllocator>());
    }

    static VarVal makeVarSizedVal(std::vector<int8_t>& buffer, const bool nullable, const bool isNull)
    {
        const VariableSizedData varData{nautilus::val<int8_t*>{buffer.data()}, nautilus::val<uint64_t>{buffer.size()}};
        return VarVal{varData, nullable, nautilus::val<bool>{isNull}};
    }

    FieldAccessPhysicalFunction readV1{Identifier::parse("v1")};
    FieldAccessPhysicalFunction readV2{Identifier::parse("v2")};
    std::shared_ptr<AbstractBufferProvider> bufferManager;
};

TEST_F(CosSimPhysicalFunctionTest, RawBufferEdgeCases)
{
    Arena arena{bufferManager};
    ArenaRef arenaRef{&arena};
    const DataType nonNullOutput{DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE};
    const CosSimPhysicalFunction cosSim{readV1, readV2, nonNullOutput};

    std::vector<int8_t> emptyBuf{};
    std::vector<int8_t> subFourByteBuf{'A', 'B'};
    std::vector<int8_t> fourByteZeroBuf{'@', '@', '@', '@'};
    std::vector<int8_t> fourByteBBuf{'B', 'B', 'B', 'B'};

    {
        Record record{std::unordered_map<Record::RecordFieldIdentifier, VarVal>{
            {Identifier::parse("v1"), makeVarSizedVal(emptyBuf, false, false)},
            {Identifier::parse("v2"), makeVarSizedVal(fourByteZeroBuf, false, false)}}};
        const auto result = cosSim.execute(record, arenaRef);
        EXPECT_EQ(result.getRawValueAs<nautilus::val<double>>(), 0.0);
    }

    {
        Record record{std::unordered_map<Record::RecordFieldIdentifier, VarVal>{
            {Identifier::parse("v1"), makeVarSizedVal(fourByteZeroBuf, false, false)},
            {Identifier::parse("v2"), makeVarSizedVal(emptyBuf, false, false)}}};
        const auto result = cosSim.execute(record, arenaRef);
        EXPECT_EQ(result.getRawValueAs<nautilus::val<double>>(), 0.0);
    }

    {
        Record record{std::unordered_map<Record::RecordFieldIdentifier, VarVal>{
            {Identifier::parse("v1"), makeVarSizedVal(subFourByteBuf, false, false)},
            {Identifier::parse("v2"), makeVarSizedVal(fourByteZeroBuf, false, false)}}};
        ASSERT_EXCEPTION_ERRORCODE((std::ignore = cosSim.execute(record, arenaRef)), ErrorCode::FormattingError);
    }

    {
        Record record{std::unordered_map<Record::RecordFieldIdentifier, VarVal>{
            {Identifier::parse("v1"), makeVarSizedVal(fourByteZeroBuf, false, false)},
            {Identifier::parse("v2"), makeVarSizedVal(subFourByteBuf, false, false)}}};
        ASSERT_EXCEPTION_ERRORCODE((std::ignore = cosSim.execute(record, arenaRef)), ErrorCode::FormattingError);
    }

    {
        Record record{std::unordered_map<Record::RecordFieldIdentifier, VarVal>{
            {Identifier::parse("v1"), makeVarSizedVal(fourByteZeroBuf, false, false)},
            {Identifier::parse("v2"), makeVarSizedVal(fourByteBBuf, false, false)}}};
        const auto result = cosSim.execute(record, arenaRef);
        EXPECT_EQ(result.getRawValueAs<nautilus::val<double>>(), 1.0);
    }
}

TEST_F(CosSimPhysicalFunctionTest, NullabilityPropagation)
{
    Arena arena{bufferManager};
    ArenaRef arenaRef{&arena};
    const DataType nullableOutput{DataType::Type::FLOAT64, DataType::NULLABLE::IS_NULLABLE};
    const CosSimPhysicalFunction cosSim{readV1, readV2, nullableOutput};

    std::vector<int8_t> fourByteZeroBuf{'@', '@', '@', '@'};

    {
        Record record{std::unordered_map<Record::RecordFieldIdentifier, VarVal>{
            {Identifier::parse("v1"), makeVarSizedVal(fourByteZeroBuf, true, false)},
            {Identifier::parse("v2"), makeVarSizedVal(fourByteZeroBuf, true, false)}}};
        const auto result = cosSim.execute(record, arenaRef);
        EXPECT_TRUE(result.isNullable());
        EXPECT_FALSE(result.isNull());
        EXPECT_EQ(result.getRawValueAs<nautilus::val<double>>(), 1.0);
    }

    {
        Record record{std::unordered_map<Record::RecordFieldIdentifier, VarVal>{
            {Identifier::parse("v1"), makeVarSizedVal(fourByteZeroBuf, true, true)},
            {Identifier::parse("v2"), makeVarSizedVal(fourByteZeroBuf, true, false)}}};
        const auto result = cosSim.execute(record, arenaRef);
        EXPECT_TRUE(result.isNull());
    }

    {
        Record record{std::unordered_map<Record::RecordFieldIdentifier, VarVal>{
            {Identifier::parse("v1"), makeVarSizedVal(fourByteZeroBuf, true, false)},
            {Identifier::parse("v2"), makeVarSizedVal(fourByteZeroBuf, true, true)}}};
        const auto result = cosSim.execute(record, arenaRef);
        EXPECT_TRUE(result.isNull());
    }

    {
        Record record{std::unordered_map<Record::RecordFieldIdentifier, VarVal>{
            {Identifier::parse("v1"), makeVarSizedVal(fourByteZeroBuf, true, true)},
            {Identifier::parse("v2"), makeVarSizedVal(fourByteZeroBuf, true, true)}}};
        const auto result = cosSim.execute(record, arenaRef);
        EXPECT_TRUE(result.isNull());
    }

    {
        Record record{std::unordered_map<Record::RecordFieldIdentifier, VarVal>{
            {Identifier::parse("v1"), makeVarSizedVal(fourByteZeroBuf, false, false)},
            {Identifier::parse("v2"), makeVarSizedVal(fourByteZeroBuf, true, false)}}};
        const auto result = cosSim.execute(record, arenaRef);
        EXPECT_TRUE(result.isNullable());
        EXPECT_FALSE(result.isNull());
        EXPECT_EQ(result.getRawValueAs<nautilus::val<double>>(), 1.0);
    }

    {
        Record record{std::unordered_map<Record::RecordFieldIdentifier, VarVal>{
            {Identifier::parse("v1"), makeVarSizedVal(fourByteZeroBuf, false, false)},
            {Identifier::parse("v2"), makeVarSizedVal(fourByteZeroBuf, true, true)}}};
        const auto result = cosSim.execute(record, arenaRef);
        EXPECT_TRUE(result.isNull());
    }
}

}
