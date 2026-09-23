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

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Interface/Record.hpp>
#include <Operators/Statistic/StatisticBlobType.hpp>
#include <Operators/Windows/Aggregations/SumAggregationLogicalFunction.hpp>
#include <Statistics/ScalarStatisticIterator.hpp>
#include <Statistics/StatisticIterator.hpp>
#include <Statistics/StatisticIteratorProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <StatisticIteratorRegistry.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{

/// A decoder registered by this test alone. The registry is a process-global singleton with no way to remove an
/// entry, so the name must not collide with a real blob type.
constexpr auto TEST_BLOB_TYPE = "StatisticIteratorRegistryTestSynopsis";

class TestSynopsisStatisticIterator final : public StatisticIterator
{
public:
    explicit TestSynopsisStatisticIterator(StatisticBlobType typeName) : StatisticIterator(std::move(typeName)) { }

    [[nodiscard]] uint64_t getExpectedPayloadSizeInBytes() const override { return 0; }

    void forEachRecord(const nautilus::val<int8_t*>&, const std::function<void(Record&)>&) const override { }
};

DataType uint64Type()
{
    return DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
}

std::vector<StatisticPayloadField> fields(const uint64_t count)
{
    std::vector<StatisticPayloadField> payloadFields;
    for (uint64_t i = 0; i < count; ++i)
    {
        payloadFields.emplace_back(Record::RecordFieldIdentifier::parse("field" + std::to_string(i)), uint64Type());
    }
    return payloadFields;
}

}

class StatisticIteratorRegistryTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("StatisticIteratorRegistryTest.log", LogLevel::LOG_DEBUG);
        ASSERT_TRUE(StatisticIteratorRegistry::instance().addEntry(
            TEST_BLOB_TYPE,
            [](StatisticIteratorRegistryArguments arguments)
            { return std::make_shared<TestSynopsisStatisticIterator>(std::move(arguments.typeName)); }));
    }
};

/// A blob type with an entry gets the registered decoder, whatever its payload columns look like.
TEST_F(StatisticIteratorRegistryTest, registeredBlobTypeGetsItsOwnIterator)
{
    const auto iterator = StatisticIteratorProvider::provide({.typeName = StatisticBlobType{TEST_BLOB_TYPE}, .payloadFields = fields(3)});

    ASSERT_NE(iterator, nullptr);
    EXPECT_NE(std::dynamic_pointer_cast<TestSynopsisStatisticIterator>(iterator), nullptr);
    EXPECT_EQ(iterator->getStatisticBlobType(), StatisticBlobType{TEST_BLOB_TYPE});
}

/// The lookup is case-insensitive, like every other runtime registry.
TEST_F(StatisticIteratorRegistryTest, registryLookupIgnoresCase)
{
    const auto iterator = StatisticIteratorProvider::provide(
        {.typeName = StatisticBlobType{"statisticiteratorregistrytestsynopsis"}, .payloadFields = fields(1)});

    EXPECT_NE(std::dynamic_pointer_cast<TestSynopsisStatisticIterator>(iterator), nullptr);
}

/// A blob type without an entry is an ordinary aggregation and decodes as a scalar.
TEST_F(StatisticIteratorRegistryTest, unregisteredBlobTypeFallsBackToTheScalarIterator)
{
    const auto iterator = StatisticIteratorProvider::provide(
        {.typeName = StatisticBlobType{SumAggregationLogicalFunction::NAME}, .payloadFields = fields(1)});

    ASSERT_NE(iterator, nullptr);
    EXPECT_NE(std::dynamic_pointer_cast<ScalarStatisticIterator>(iterator), nullptr);
    EXPECT_EQ(iterator->getExpectedPayloadSizeInBytes(), uint64Type().getSizeInBytesWithoutNull());
}

/// The number of columns comes from the query, so getting it wrong is a user error, not an invariant violation.
TEST_F(StatisticIteratorRegistryTest, scalarIteratorRejectsAnyColumnCountButOne)
{
    for (const auto columnCount : {0U, 2U, 5U})
    {
        try
        {
            StatisticIteratorProvider::provide(
                {.typeName = StatisticBlobType{SumAggregationLogicalFunction::NAME}, .payloadFields = fields(columnCount)});
            ADD_FAILURE() << "expected " << columnCount << " columns to be rejected";
        }
        catch (const Exception& exception)
        {
            EXPECT_EQ(exception.code(), ErrorCode::InvalidQuerySyntax) << "for " << columnCount << " columns";
        }
    }
}

/// Nothing can decode a foreign payload without being asked to, so the base class accepts every payload it is not
/// taught to reject.
TEST_F(StatisticIteratorRegistryTest, theDefaultValidateAcceptsEveryPayload)
{
    const auto iterator = StatisticIteratorProvider::provide(
        {.typeName = StatisticBlobType{SumAggregationLogicalFunction::NAME}, .payloadFields = fields(1)});

    EXPECT_NO_THROW(iterator->validate({}));
}

}
