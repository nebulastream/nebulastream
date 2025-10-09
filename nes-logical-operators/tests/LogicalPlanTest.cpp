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

#include <utility>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/Trait.hpp>

using namespace NES;

class LogicalPlanTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        /// Create some test operators
        sourceOp = SourceNameLogicalOperator("Source");
        auto dummySchema = Schema{};
        auto logicalSource = sourceCatalog.addLogicalSource("Source", dummySchema).value(); /// NOLINT
        auto dummyParserConfig = ParserConfig{.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","};
        auto dummySourceDescriptor = sourceCatalog /// NOLINT
                                         .addPhysicalSource(logicalSource, "File", {{"file_path", "/dev/null"}}, dummyParserConfig)
                                         .value();
        sourceOp2 = SourceDescriptorLogicalOperator(std::move(dummySourceDescriptor));
        selectionOp = SelectionLogicalOperator(FieldAccessLogicalFunction("logicalfunction"));
        sinkOp = SinkLogicalOperator();
    }

    SourceCatalog sourceCatalog;
    LogicalOperator sourceOp;
    LogicalOperator sourceOp2;
    LogicalOperator selectionOp;
    LogicalOperator sinkOp;
};

TEST_F(LogicalPlanTest, DefaultConstructor)
{
    const LogicalPlan plan;
    EXPECT_TRUE(plan.getRootOperators().empty());
    EXPECT_EQ(plan.getQueryId(), INVALID_QUERY_ID);
}

TEST_F(LogicalPlanTest, SingleRootConstructor)
{
    LogicalPlan plan(sourceOp);
    EXPECT_EQ(plan.getRootOperators().size(), 1);
    EXPECT_EQ(plan.getRootOperators()[0], sourceOp);
}

TEST_F(LogicalPlanTest, MultipleRootsConstructor)
{
    const std::vector roots = {sourceOp, selectionOp};
    const auto queryId = QueryId(1);
    LogicalPlan plan(queryId, roots);
    EXPECT_EQ(plan.getRootOperators().size(), 2);
    EXPECT_EQ(plan.getQueryId(), queryId);
    EXPECT_EQ(plan.getRootOperators()[0], sourceOp);
    EXPECT_EQ(plan.getRootOperators()[1], selectionOp);
}

TEST_F(LogicalPlanTest, CopyConstructor)
{
    const LogicalPlan original(sourceOp);
    const LogicalPlan& copy(original);
    EXPECT_EQ(copy.getRootOperators().size(), 1);
    EXPECT_EQ(copy.getRootOperators()[0], sourceOp);
}

TEST_F(LogicalPlanTest, PromoteOperatorToRoot)
{
    const LogicalOperator sourceOp{SourceNameLogicalOperator("source")};
    const LogicalOperator selectionOp{SelectionLogicalOperator(FieldAccessLogicalFunction("field"))};
    const auto plan = LogicalPlan(sourceOp);
    const auto promoteResultPlan = promoteOperatorToRoot(plan, selectionOp);
    EXPECT_EQ(*promoteResultPlan.getRootOperators()[0], *selectionOp);
    EXPECT_EQ(*promoteResultPlan.getRootOperators()[0].getChildren()[0], *sourceOp);
}

TEST_F(LogicalPlanTest, ReplaceOperator)
{
    const LogicalOperator sourceOp{SourceNameLogicalOperator("source")};
    const LogicalOperator sourceOp2{SourceNameLogicalOperator("source2")};
    const auto plan = LogicalPlan(sourceOp);
    const auto result = replaceOperator(plan, sourceOp.getId(), sourceOp2);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(*result->getRootOperators()[0], *sourceOp2); ///NOLINT(bugprone-unchecked-optional-access)
}

TEST_F(LogicalPlanTest, replaceSubtree)
{
    const LogicalOperator sourceOp{SourceNameLogicalOperator("source")};
    const LogicalOperator sourceOp2{SourceNameLogicalOperator("source2")};
    const auto plan = LogicalPlan(sourceOp);
    const auto result = replaceSubtree(plan, sourceOp.getId(), sourceOp2);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->getRootOperators()[0].getId(), sourceOp2.getId()); ///NOLINT(bugprone-unchecked-optional-access)
}

TEST_F(LogicalPlanTest, GetParents)
{
    const LogicalOperator sourceOp{SourceNameLogicalOperator("source")};
    const LogicalOperator selectionOp{SelectionLogicalOperator(FieldAccessLogicalFunction("field"))};
    const auto plan = LogicalPlan(sourceOp);
    const auto promoteResult = promoteOperatorToRoot(plan, selectionOp);
    const auto parents = getParents(promoteResult, sourceOp);
    EXPECT_EQ(parents.size(), 1);
    EXPECT_EQ(*parents[0], *selectionOp);
}

TEST_F(LogicalPlanTest, GetOperatorByType)
{
    const auto sourceOp = LogicalOperator{SourceNameLogicalOperator("source")};
    const auto plan = LogicalPlan(sourceOp);
    const auto sourceOperators = getOperatorByType<SourceNameLogicalOperator>(plan);
    EXPECT_EQ(sourceOperators.size(), 1);
    EXPECT_EQ(*LogicalOperator{sourceOperators[0]}, *sourceOp);
}

TEST_F(LogicalPlanTest, GetOperatorsById)
{
    const LogicalOperator sourceOp{SourceNameLogicalOperator{"TestSource"}};
    const LogicalOperator selectionOp{SelectionLogicalOperator{FieldAccessLogicalFunction{"fn"}}.withChildren({sourceOp})};
    const LogicalOperator sinkOp{SinkLogicalOperator{"TestSink"}.withChildren({selectionOp})};
    const LogicalPlan plan(sinkOp);
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

TEST_F(LogicalPlanTest, AddTraits)
{
    EXPECT_TRUE(std::ranges::empty(sourceOp.getTraitSet()));
    const auto sourceWithTrait = sourceOp.withTraitSet(TraitSet{TestTrait{}});
    auto sourceTraitSet = sourceWithTrait.getTraitSet();
    EXPECT_EQ(std::ranges::size(sourceTraitSet), 1);
    EXPECT_TRUE(sourceTraitSet.tryGet<TestTrait>().has_value());
}

TEST_F(LogicalPlanTest, GetLeafOperators)
{
    /// source1 -> selection -> source2
    std::vector children = {sourceOp2};
    selectionOp = selectionOp.withChildren(children);
    children = {selectionOp};
    sourceOp = sourceOp.withChildren(children);
    const LogicalPlan plan(sourceOp);
    auto leafOperators = getLeafOperators(plan);
    EXPECT_EQ(leafOperators.size(), 1);
    EXPECT_EQ(leafOperators[0], sourceOp2);
}

TEST_F(LogicalPlanTest, GetAllOperators)
{
    /// source1 -> selection -> source2
    std::vector children = {sourceOp2};
    selectionOp = selectionOp.withChildren(children);
    children = {selectionOp};
    sourceOp = sourceOp.withChildren(children);
    const LogicalPlan plan(sourceOp);
    const auto allOperators = flatten(plan);
    EXPECT_EQ(allOperators.size(), 3);
    EXPECT_TRUE(allOperators.contains(sourceOp));
    EXPECT_TRUE(allOperators.contains(selectionOp));
    EXPECT_TRUE(allOperators.contains(sourceOp2));
}

TEST_F(LogicalPlanTest, EqualityOperator)
{
    const LogicalPlan plan1(sourceOp);
    const LogicalPlan plan2(sourceOp);
    EXPECT_TRUE(plan1 == plan2);

    const LogicalPlan plan3(selectionOp);
    EXPECT_FALSE(plan1 == plan3);
}

TEST_F(LogicalPlanTest, OutputOperator)
{
    const LogicalPlan plan(sourceOp);
    std::stringstream ss;
    ss << plan;
    EXPECT_FALSE(ss.str().empty());
}
