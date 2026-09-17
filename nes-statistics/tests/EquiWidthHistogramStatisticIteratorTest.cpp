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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Interface/Record.hpp>
#include <Operators/Statistic/StatisticBlobType.hpp>
#include <Operators/Windows/Aggregations/EquiWidthHistogramAggregationLogicalFunction.hpp>
#include <Statistics/EquiWidthHistogramStatisticIterator.hpp>
#include <Statistics/StatisticIterator.hpp>
#include <Statistics/StatisticIteratorProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <StatisticIteratorRegistry.hpp>

namespace NES
{
namespace
{

DataType uint64Type(const DataType::NULLABLE nullable = DataType::NULLABLE::NOT_NULLABLE)
{
    return DataTypeProvider::provideDataType(DataType::Type::UINT64, nullable);
}

StatisticPayloadField payloadField(const std::string& name, const DataType type)
{
    return StatisticPayloadField{Record::RecordFieldIdentifier::parse(name), type};
}

std::vector<StatisticPayloadField> binColumns()
{
    return {payloadField("binStart", uint64Type()), payloadField("binCounter", uint64Type()), payloadField("binEnd", uint64Type())};
}

StatisticIteratorRegistryArguments probeWith(std::vector<StatisticPayloadField> payloadFields)
{
    return {.typeName = StatisticBlobType{EquiWidthHistogramAggregationLogicalFunction::NAME}, .payloadFields = std::move(payloadFields)};
}

ErrorCode errorOf(std::vector<StatisticPayloadField> payloadFields)
{
    try
    {
        std::ignore = StatisticIteratorProvider::provide(probeWith(std::move(payloadFields)));
    }
    catch (const Exception& exception)
    {
        return exception.code();
    }
    ADD_FAILURE() << "expected the probe to be rejected";
    return ErrorCode::UnknownException;
}

}

class EquiWidthHistogramStatisticIteratorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("EquiWidthHistogramStatisticIteratorTest.log", LogLevel::LOG_DEBUG); }
};

/// The histogram is a registry entry, so a probe of its blob type gets its decoder instead of the scalar fallback.
TEST_F(EquiWidthHistogramStatisticIteratorTest, theRegistryHandsOutTheHistogramDecoder)
{
    const auto iterator = StatisticIteratorProvider::provide(probeWith(binColumns()));

    ASSERT_NE(iterator, nullptr);
    EXPECT_NE(std::dynamic_pointer_cast<EquiWidthHistogramStatisticIterator>(iterator), nullptr);
    EXPECT_EQ(iterator->getStatisticBlobType(), StatisticBlobType{EquiWidthHistogramAggregationLogicalFunction::NAME});
    /// A histogram payload is as long as its own header says, so the probe's fixed-size check does not apply to it.
    EXPECT_EQ(iterator->getExpectedPayloadSizeInBytes(), 0U);
}

/// The columns come from the query, so a probe that does not describe a bin is a user error with a message that
/// spells out the call it should have been.
TEST_F(EquiWidthHistogramStatisticIteratorTest, aProbeThatIsNotThreeUnsignedColumnsIsRejected)
{
    auto twoColumns = binColumns();
    twoColumns.pop_back();
    EXPECT_EQ(errorOf(twoColumns), ErrorCode::InvalidQuerySyntax) << "two columns";

    auto fourColumns = binColumns();
    fourColumns.push_back(payloadField("extra", uint64Type()));
    EXPECT_EQ(errorOf(fourColumns), ErrorCode::InvalidQuerySyntax) << "four columns";

    EXPECT_EQ(errorOf({}), ErrorCode::InvalidQuerySyntax) << "no columns";

    auto floatCounter = binColumns();
    floatCounter[1].type = DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE);
    EXPECT_EQ(errorOf(floatCounter), ErrorCode::InvalidQuerySyntax) << "a float64 counter";

    auto narrowStart = binColumns();
    narrowStart[0].type = DataTypeProvider::provideDataType(DataType::Type::UINT32, DataType::NULLABLE::NOT_NULLABLE);
    EXPECT_EQ(errorOf(narrowStart), ErrorCode::InvalidQuerySyntax) << "a uint32 start";

    auto nullableEnd = binColumns();
    nullableEnd[2].type = uint64Type(DataType::NULLABLE::IS_NULLABLE);
    EXPECT_EQ(errorOf(nullableEnd), ErrorCode::InvalidQuerySyntax) << "a nullable end";
}

/// The message names the call, since the user has to be able to write the right one from it.
TEST_F(EquiWidthHistogramStatisticIteratorTest, theRejectionNamesTheExpectedCall)
{
    try
    {
        std::ignore = StatisticIteratorProvider::provide(probeWith({payloadField("value", uint64Type())}));
        ADD_FAILURE() << "expected a single column to be rejected";
    }
    catch (const Exception& exception)
    {
        EXPECT_NE(std::string{exception.what()}.find("EQUIWIDTHHISTOGRAM_PROBE"), std::string::npos) << exception.what();
    }
}

}
