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
#include <ranges>
#include <string>
#include <unordered_map>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Statistic/EquiWidthHistogramBlobLayout.hpp>
#include <Operators/Windows/Aggregations/EquiWidthHistogramAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <gtest/gtest.h>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

LogicalFunction field(const std::string& name)
{
    return UnboundFieldAccessLogicalFunction{Identifier::parse(name)};
}

LogicalFunction unsignedConstant(const uint64_t value)
{
    return ConstantValueLogicalFunction{
        DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE), std::to_string(value)};
}

/// EQUIWIDTHHISTOGRAM(statisticId, field, budget, min, max) minus the statisticId, which the parser strips.
AggregationLogicalFunctionRegistryArguments
call(const std::string& fieldName, const uint64_t budget, const uint64_t minValue, const uint64_t maxValue)
{
    return {.parameters = {field(fieldName), unsignedConstant(budget), unsignedConstant(minValue), unsignedConstant(maxValue)}};
}

ErrorCode errorOf(const AggregationLogicalFunctionRegistryArguments& arguments)
{
    try
    {
        std::ignore = EquiWidthHistogramAggregationLogicalFunction::create(arguments);
    }
    catch (const Exception& exception)
    {
        return exception.code();
    }
    ADD_FAILURE() << "expected the call to be rejected";
    return ErrorCode::UnknownException;
}

}

class EquiWidthHistogramAggregationLogicalFunctionTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("EquiWidthHistogramAggregationLogicalFunctionTest.log", LogLevel::LOG_DEBUG); }

    /// A file source over one field per input type the histogram has an opinion about, so that field access can be
    /// bound to something.
    static TypedLogicalOperator<SourceDescriptorLogicalOperator> source()
    {
        const auto notNullable = DataType::NULLABLE::NOT_NULLABLE;
        const Schema<UnqualifiedUnboundField, Ordered> sourceSchema{
            UnqualifiedUnboundField{
                Identifier::parse("unsignedValue"), DataTypeProvider::provideDataType(DataType::Type::UINT64, notNullable)},
            UnqualifiedUnboundField{
                Identifier::parse("narrowValue"), DataTypeProvider::provideDataType(DataType::Type::UINT8, notNullable)},
            UnqualifiedUnboundField{
                Identifier::parse("signedValue"), DataTypeProvider::provideDataType(DataType::Type::INT64, notNullable)},
            UnqualifiedUnboundField{
                Identifier::parse("floatValue"), DataTypeProvider::provideDataType(DataType::Type::FLOAT64, notNullable)},
            UnqualifiedUnboundField{
                Identifier::parse("textValue"), DataTypeProvider::provideDataType(DataType::Type::VARSIZED, notNullable)},
            UnqualifiedUnboundField{
                Identifier::parse("nullableValue"),
                DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE)}};

        const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("file_path"), "/dev/null"}};
        const std::unordered_map<Identifier, std::string> parserConfig{
            {Identifier::parse("type"), "CSV"}, {Identifier::parse("tuple_delimiter"), "\n"}, {Identifier::parse("field_delimiter"), ","}};
        auto descriptor = SourceDescriptor::create(
            PhysicalSourceId{1},
            LogicalSource{Identifier::parse("source"), sourceSchema},
            Identifier::parse("File"),
            Host("localhost"),
            sourceConfig,
            parserConfig,
            false);
        return SourceDescriptorLogicalOperator::create(descriptor.value());
    }

    /// The source's bound output schema, for withInferredType to resolve field access against.
    static Schema<Field, Unordered> boundSchema() { return source().withInferredSchema().getOutputSchema(); }

    static EquiWidthHistogramAggregationLogicalFunction inferOver(const std::string& fieldName)
    {
        const auto function
            = EquiWidthHistogramAggregationLogicalFunction{field(fieldName).getAs<UnboundFieldAccessLogicalFunction>(), 12, 0, 25};
        return function.withInferredType(boundSchema());
    }

    static ErrorCode inferenceErrorOver(const std::string& fieldName)
    {
        try
        {
            std::ignore = inferOver(fieldName);
        }
        catch (const Exception& exception)
        {
            return exception.code();
        }
        ADD_FAILURE() << "expected " << fieldName << " to be rejected";
        return ErrorCode::UnknownException;
    }
};

/// The SQL call takes a budget; what the function keeps is the bin count that budget bought.
TEST_F(EquiWidthHistogramAggregationLogicalFunctionTest, aBudgetBecomesABinCount)
{
    const auto function = EquiWidthHistogramAggregationLogicalFunction::create(call("value", 120, 0, 25))
                              .tryGetAs<EquiWidthHistogramAggregationLogicalFunction>();
    ASSERT_TRUE(function.has_value());
    EXPECT_EQ(function.value()->getNumberOfBins(), equiWidthHistogramBinsForBudget(120));
    EXPECT_EQ(function.value()->getNumberOfBins(), 12U);
    EXPECT_EQ(function.value()->getMinValue(), 0U);
    EXPECT_EQ(function.value()->getMaxValue(), 25U);
    EXPECT_EQ(function.value()->getName(), EquiWidthHistogramAggregationLogicalFunction::NAME);
    EXPECT_EQ(EquiWidthHistogramAggregationLogicalFunction::getAggregateType().type, DataType::Type::VARSIZED);
    EXPECT_FALSE(EquiWidthHistogramAggregationLogicalFunction::getAggregateType().nullable);
}

/// Everything about the call the parser cannot check for itself.
TEST_F(EquiWidthHistogramAggregationLogicalFunctionTest, aBadCallIsInvalidQuerySyntax)
{
    EXPECT_EQ(errorOf({.parameters = {field("value"), unsignedConstant(120), unsignedConstant(0)}}), ErrorCode::InvalidQuerySyntax)
        << "too few arguments";
    EXPECT_EQ(
        errorOf({.parameters = {field("value"), unsignedConstant(120), unsignedConstant(0), unsignedConstant(25), unsignedConstant(1)}}),
        ErrorCode::InvalidQuerySyntax)
        << "too many arguments";
    EXPECT_EQ(
        errorOf({.parameters = {unsignedConstant(1), unsignedConstant(120), unsignedConstant(0), unsignedConstant(25)}}),
        ErrorCode::InvalidQuerySyntax)
        << "a constant where the field belongs";
    EXPECT_EQ(
        errorOf({.parameters = {field("value"), field("budget"), unsignedConstant(0), unsignedConstant(25)}}),
        ErrorCode::InvalidQuerySyntax)
        << "a field where the budget belongs";

    EXPECT_EQ(errorOf(call("value", 120, 25, 25)), ErrorCode::InvalidQuerySyntax) << "an empty range";
    EXPECT_EQ(errorOf(call("value", 120, 25, 0)), ErrorCode::InvalidQuerySyntax) << "an inverted range";
    EXPECT_EQ(errorOf(call("value", EquiWidthHistogramBlob::HEADER_SIZE, 0, 25)), ErrorCode::InvalidQuerySyntax)
        << "a budget below one bin";
}

/// A budget that buys more bins than the range holds values would make the bin width zero. The caller only ever
/// stated a budget, so the message has to name the budget that would have worked.
TEST_F(EquiWidthHistogramAggregationLogicalFunctionTest, tooManyBinsForTheRangeNamesTheLargestBudget)
{
    try
    {
        std::ignore = EquiWidthHistogramAggregationLogicalFunction::create(call("value", 1024, 0, 25));
        ADD_FAILURE() << "expected 125 bins over a range of 25 to be rejected";
    }
    catch (const Exception& exception)
    {
        EXPECT_EQ(exception.code(), ErrorCode::InvalidQuerySyntax);
        EXPECT_NE(std::string{exception.what()}.find(std::to_string(equiWidthHistogramBudgetForBins(25))), std::string::npos)
            << exception.what();
    }

    /// And that budget is accepted, so the message does not send the caller into a second rejection.
    EXPECT_NO_THROW(
        std::ignore = EquiWidthHistogramAggregationLogicalFunction::create(call("value", equiWidthHistogramBudgetForBins(25), 0, 25)));
}

/// A plan travels to the worker as reflected data, so the three numbers that define the histogram have to survive
/// the round trip -- which they only do if the aggregation is registered for unreflection under its name.
TEST_F(EquiWidthHistogramAggregationLogicalFunctionTest, planSerializationKeepsTheGeometry)
{
    const auto aggregation = WindowedAggregationLogicalOperator::create(
        source(),
        WindowedAggregationLogicalOperator::GroupingKeyType{},
        {WindowedAggregationLogicalOperator::ProjectedAggregation{
            .function = EquiWidthHistogramAggregationLogicalFunction::create(call("unsignedValue", 120, 7, 99)),
            .name = Identifier::parse("histogram")}},
        Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{1000}}},
        Windowing::TimeCharacteristic{Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}}});
    const auto inferredAggregation = aggregation->withInferredSchema();

    /// A plan only deserializes from a sink down, so the aggregation needs one, declared over its own output.
    const auto sinkSchema = Schema<UnqualifiedUnboundField, Ordered>{
        inferredAggregation.getOutputSchema() | std::views::transform([](const Field& field) { return field.unbound(); })
        | std::ranges::to<std::vector>()};
    const std::unordered_map<Identifier, std::string> sinkConfig{
        {Identifier::parse("FILE_PATH"), "/dev/null"}, {Identifier::parse("OUTPUT_FORMAT"), "CSV"}};
    const auto sinkDescriptor = SinkDescriptor::createNamed(
        INVALID_SINK_ID, Identifier::parse("sink"), Identifier::parse("file"), sinkSchema, Host{"localhost"}, sinkConfig, {});
    const auto sink = SinkLogicalOperator::create(sinkDescriptor.value()).withChildrenUnsafe({inferredAggregation});
    const LogicalPlan plan{QueryId{1}, {sink->withInferredSchema()}};

    const auto restored = QueryPlanSerializationUtil::deserializeQueryPlan(QueryPlanSerializationUtil::serializeQueryPlan(plan));

    const auto aggregations = getOperatorByType<WindowedAggregationLogicalOperator>(restored);
    ASSERT_EQ(aggregations.size(), 1U);
    const auto histogram
        = aggregations.front()->getWindowAggregation().front().function.tryGetAs<EquiWidthHistogramAggregationLogicalFunction>();
    ASSERT_TRUE(histogram.has_value());
    EXPECT_EQ(histogram.value()->getNumberOfBins(), 12U);
    EXPECT_EQ(histogram.value()->getMinValue(), 7U);
    EXPECT_EQ(histogram.value()->getMaxValue(), 99U);
}

TEST_F(EquiWidthHistogramAggregationLogicalFunctionTest, anUnsignedFieldIsAccepted)
{
    for (const auto* fieldName : {"unsignedValue", "narrowValue"})
    {
        const auto function = inferOver(fieldName);
        EXPECT_EQ(function.getNumberOfBins(), 12U) << fieldName;
        EXPECT_EQ(function.getInputFunction().index(), 0U) << fieldName << " should be bound after inference";
    }
}

/// The bins are laid out over unsigned integers and lift() bins the raw value, so anything else would have to be
/// converted first -- and a converted value is not the value the user asked about.
TEST_F(EquiWidthHistogramAggregationLogicalFunctionTest, anythingButAnUnsignedFieldIsRejected)
{
    for (const auto* fieldName : {"signedValue", "floatValue", "textValue", "nullableValue"})
    {
        EXPECT_EQ(inferenceErrorOver(fieldName), ErrorCode::CannotInferSchema) << fieldName;
    }
}

}
