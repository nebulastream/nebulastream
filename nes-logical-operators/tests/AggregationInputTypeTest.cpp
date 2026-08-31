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

#include <algorithm>
#include <array>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>

#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

constexpr std::array NonNumericAggregations{std::string_view{"COUNT"}};

class AggregationInputTypeTest : public ::testing::Test
{
public:
    static Schema<UnqualifiedUnboundField, Ordered> createSchema()
    {
        return Schema<UnqualifiedUnboundField, Ordered>{
            {Identifier::parse("key"), DataTypeProvider::provideDataType(DataType::Type::VARSIZED)},
            {Identifier::parse("i8"), DataTypeProvider::provideDataType(DataType::Type::INT8)}};
    }

    static TypedLogicalOperator<SourceDescriptorLogicalOperator> makeSource(SourceCatalog& catalog)
    {
        const auto logical = catalog.addLogicalSource(Identifier::parse("agg_src"), createSchema()).value();
        const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("file_path"), "/dev/null"}};
        const std::unordered_map<Identifier, std::string> parserConfig{{Identifier::parse("type"), "CSV"}};
        const auto descriptor
            = catalog.addPhysicalSource(logical, Identifier::parse("file"), Host{"localhost"}, sourceConfig, parserConfig).value();
        return SourceDescriptorLogicalOperator::create(descriptor);
    }

    static WindowAggregationLogicalFunction createAggregation(const std::string& name, const Identifier& field)
    {
        const TypedLogicalFunction<UnboundFieldAccessLogicalFunction> fieldAccess{UnboundFieldAccessLogicalFunction{field}};
        return AggregationLogicalFunctionRegistry::instance().find(name).value()(
            AggregationLogicalFunctionRegistryArguments{.on = {fieldAccess}, .includeNullValues = false});
    }
};

TEST_F(AggregationInputTypeTest, RejectsNonNumericInput)
{
    SourceCatalog catalog;
    const auto schema = makeSource(catalog)->getOutputSchema();

    for (const auto& name : AggregationLogicalFunctionRegistry::instance().getRegisteredNames())
    {
        if (std::ranges::contains(NonNumericAggregations, name))
        {
            continue;
        }
        SCOPED_TRACE(name);
        try
        {
            std::ignore = createAggregation(name, Identifier::parse("key")).withInferredType(schema);
            ADD_FAILURE() << "Expected an UnsupportedQuery exception";
        }
        catch (const Exception& exception)
        {
            EXPECT_EQ(exception.code(), ErrorCode::UnsupportedQuery);
            EXPECT_TRUE(toUpperCase(exception.what()).contains(fmt::format("{} IS ONLY SUPPORTED ON NUMERIC FIELDS", name)));
        }
    }
}

TEST_F(AggregationInputTypeTest, AcceptsNumericInput)
{
    SourceCatalog catalog;
    const auto schema = makeSource(catalog)->getOutputSchema();

    for (const auto& name : AggregationLogicalFunctionRegistry::instance().getRegisteredNames())
    {
        SCOPED_TRACE(name);
        EXPECT_NO_THROW(std::ignore = createAggregation(name, Identifier::parse("i8")).withInferredType(schema));
    }
}
}
}
