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
#include <ranges>
#include <sstream>

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <Identifiers/Identifier.hpp>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/Trait.hpp>
#include <Util/Reflection.hpp>
#include <Util/UUID.hpp>
#include <QueryId.hpp>

using namespace NES;

QueryId randomQueryId()
{
    return QueryId::createLocal(LocalQueryId(generateUUID()));
}

class LogicalPlanTest : public ::testing::Test
{
public:
    explicit LogicalPlanTest()
        : testFieldIdentifier(Identifier::parse("testField"))
        , sourceOp{Identifier::parse("Source")}
        , sourceOp2{[this]
                    {
                        const auto dummySchema = Schema<UnqualifiedUnboundField, Ordered>{
                            UnqualifiedUnboundField{testFieldIdentifier, DataTypeProvider::provideDataType(DataType::Type::UINT64)}};
                        auto logicalSource = sourceCatalog.addLogicalSource(Identifier::parse("Source"), dummySchema).value(); /// NOLINT
                        const Schema<LiteralConfigValue, Ordered> dummyParserConfig{
                            {"type", "CSV"}, {"tuple_delimiter", "\n"}, {"field_delimiter", ","}};
                        return sourceCatalog /// NOLINT
                            .addPhysicalSource(
                                logicalSource,
                                Identifier::parse("File"),
                                Schema<LiteralConfigValue, Ordered>{{"file_path", "/dev/null"}, {"host", "localhost"}},
                                dummyParserConfig)
                            .value();
                    }()}
        , selectionOp{UnboundFieldAccessLogicalFunction{testFieldIdentifier}}

    {
    }

protected:
    void SetUp() override { }

    Identifier testFieldIdentifier;
    SourceCatalog sourceCatalog;
    TypedLogicalOperator<SourceNameLogicalOperator> sourceOp;
    TypedLogicalOperator<SourceDescriptorLogicalOperator> sourceOp2;
    TypedLogicalOperator<SelectionLogicalOperator> selectionOp;
};

TEST_F(LogicalPlanTest, SingleRootConstructor)
{
    const LogicalPlan plan(INVALID_QUERY_ID, {sourceOp});
    EXPECT_EQ(plan.getRootOperators().size(), 1);
    EXPECT_EQ(plan.getRootOperators()[0], sourceOp);
}

TEST_F(LogicalPlanTest, MultipleRootsConstructor)
{
    const std::vector<LogicalOperator> roots = {sourceOp, selectionOp};
    const auto queryId = randomQueryId();
    LogicalPlan plan(queryId, roots);
    EXPECT_EQ(plan.getRootOperators().size(), 2);
    EXPECT_EQ(plan.getQueryId(), queryId);
    EXPECT_EQ(plan.getRootOperators()[0], sourceOp);
    EXPECT_EQ(plan.getRootOperators()[1], selectionOp);
}

TEST_F(LogicalPlanTest, CopyConstructor)
{
    const LogicalPlan original(INVALID_QUERY_ID, {sourceOp});
    const LogicalPlan& copy(original);
    EXPECT_EQ(copy.getRootOperators().size(), 1);
    EXPECT_EQ(copy.getRootOperators()[0], sourceOp);
}

TEST_F(LogicalPlanTest, GetParents)
{
    const auto sourceOp = SourceNameLogicalOperator::create(Identifier::parse("source"));
    const auto selectionOp
        = SelectionLogicalOperator::create(UnboundFieldAccessLogicalFunction(Identifier::parse("field"))).withChildrenUnsafe({sourceOp});
    const auto plan = LogicalPlan(INVALID_QUERY_ID, {selectionOp});
    const auto parents = getParents(plan, sourceOp);
    EXPECT_EQ(parents.size(), 1);
    EXPECT_EQ(parents[0], selectionOp);
}

TEST_F(LogicalPlanTest, GetOperatorByType)
{
    const auto sourceOp = SourceNameLogicalOperator::create(Identifier::parse("source"));
    const auto plan = LogicalPlan(INVALID_QUERY_ID, {sourceOp});
    const auto sourceOperators = getOperatorByType<SourceNameLogicalOperator>(plan);
    EXPECT_EQ(sourceOperators.size(), 1);
    EXPECT_EQ(LogicalOperator{sourceOperators[0]}, sourceOp);
}

TEST_F(LogicalPlanTest, GetOperatorsById)
{
    const auto sourceOp = SourceNameLogicalOperator::create(Identifier::parse("TestSource"));
    const auto selectionOp = SelectionLogicalOperator::create(UnboundFieldAccessLogicalFunction{Identifier::parse("fn")})
                                 .withChildrenUnsafe(std::vector<LogicalOperator>{sourceOp});
    const LogicalOperator sinkOp{SinkLogicalOperator::create(Identifier::parse("TestSink")).withChildrenUnsafe({selectionOp})};
    const LogicalPlan plan(INVALID_QUERY_ID, {sinkOp});
    const auto op1 = getOperatorById(plan, sourceOp.getId());
    EXPECT_TRUE(op1.has_value());
    ///NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    EXPECT_EQ(op1.value().getId(), sourceOp.getId());
    const auto op2 = getOperatorById(plan, selectionOp.getId());
    EXPECT_TRUE(op2.has_value());
    EXPECT_EQ(op2.value().getId(), selectionOp.getId());
    const auto op3 = getOperatorById(plan, sinkOp.getId());
    EXPECT_TRUE(op3.has_value());
    EXPECT_EQ(op3.value().getId(), sinkOp.getId());
}

namespace
{
struct TestTrait final : DefaultTrait<TestTrait>
{
    static constexpr std::string_view NAME = "TestTrait";

    [[nodiscard]] std::string_view getName() const override { return NAME; }
};
}

template <>
struct Reflector<TestTrait>
{
    Reflected operator()(const TestTrait&) const { return Reflected{}; };
};

template <>
struct Unreflector<TestTrait>
{
    TestTrait operator()(const Reflected&) const { return TestTrait{}; };
};

TEST_F(LogicalPlanTest, AddTraits)
{
    EXPECT_TRUE(std::ranges::empty(sourceOp.getTraitSet()));
    const auto sourceWithTrait = sourceOp->withTraitSet(TraitSet{TestTrait{}});
    auto sourceTraitSet = sourceWithTrait.getTraitSet();
    EXPECT_EQ(std::ranges::size(sourceTraitSet), 1);
    EXPECT_TRUE(sourceTraitSet.tryGet<TestTrait>().has_value());
}

TEST_F(LogicalPlanTest, GetLeafOperators)
{
    /// source1 -> selection -> source2
    std::vector<LogicalOperator> children = {sourceOp2};
    selectionOp = selectionOp->withChildrenUnsafe(children);
    children = {selectionOp};
    sourceOp = sourceOp->withChildrenUnsafe(children);
    const LogicalPlan plan(INVALID_QUERY_ID, {sourceOp});
    auto leafOperators = getLeafOperators(plan);
    EXPECT_EQ(leafOperators.size(), 1);
    EXPECT_EQ(leafOperators[0], sourceOp2);
}

TEST_F(LogicalPlanTest, GetAllOperators)
{
    /// source1 -> selection -> source2
    std::vector<LogicalOperator> children = {sourceOp2};
    selectionOp = selectionOp->withChildrenUnsafe(children);
    children = {selectionOp};
    sourceOp = sourceOp->withChildrenUnsafe(children);
    const LogicalPlan plan(INVALID_QUERY_ID, {sourceOp});
    const auto allOperators = flatten(plan);
    EXPECT_EQ(allOperators.size(), 3);
    EXPECT_TRUE(allOperators.contains(sourceOp));
    EXPECT_TRUE(allOperators.contains(selectionOp));
    EXPECT_TRUE(allOperators.contains(sourceOp2));
}

TEST_F(LogicalPlanTest, EqualityOperator)
{
    const LogicalPlan plan1(INVALID_QUERY_ID, {sourceOp});
    const LogicalPlan plan2(INVALID_QUERY_ID, {sourceOp});
    EXPECT_TRUE(plan1 == plan2);

    const LogicalPlan plan3(INVALID_QUERY_ID, {selectionOp});
    EXPECT_FALSE(plan1 == plan3);
}

TEST_F(LogicalPlanTest, OutputOperator)
{
    const LogicalPlan plan(INVALID_QUERY_ID, {sourceOp});
    std::stringstream ss;
    ss << plan;
    EXPECT_FALSE(ss.str().empty());
}
