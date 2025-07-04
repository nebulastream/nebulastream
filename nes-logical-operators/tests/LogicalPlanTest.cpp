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

#include <memory>
#include <sstream>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/OriginIdAssignerTrait.hpp>
#include <gtest/gtest.h>

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
        auto dummySourceDescriptor
            = sourceCatalog /// NOLINT
                  .addPhysicalSource(
                      logicalSource, "File", {{"location", INITIAL<WorkerId>.toString()}, {"filePath", "/dev/null"}}, dummyParserConfig)
                  .value();
        sourceOp2 = SourceDescriptorLogicalOperator(std::move(dummySourceDescriptor));
        selectionOp = SelectionLogicalOperator(FieldAccessLogicalFunction("logicalfunction"));
    }

    SourceCatalog sourceCatalog;
    LogicalOperator sourceOp;
    LogicalOperator sourceOp2;
    LogicalOperator selectionOp;
};

TEST_F(LogicalPlanTest, DefaultConstructor)
{
    const LogicalPlan plan;
    EXPECT_TRUE(plan.rootOperators.empty());
    EXPECT_EQ(plan.getQueryId(), INVALID_QUERY_ID);
}

TEST_F(LogicalPlanTest, SingleRootConstructor)
{
    LogicalPlan plan(sourceOp);
    EXPECT_EQ(plan.rootOperators.size(), 1);
    EXPECT_EQ(plan.rootOperators[0], sourceOp);
}

TEST_F(LogicalPlanTest, MultipleRootsConstructor)
{
    const std::vector roots = {sourceOp, selectionOp};
    const auto queryId = QueryId(1);
    LogicalPlan plan(queryId, roots);
    EXPECT_EQ(plan.rootOperators.size(), 2);
    EXPECT_EQ(plan.getQueryId(), queryId);
    EXPECT_EQ(plan.rootOperators[0], sourceOp);
    EXPECT_EQ(plan.rootOperators[1], selectionOp);
}

TEST_F(LogicalPlanTest, CopyConstructor)
{
    const LogicalPlan original(sourceOp);
    const LogicalPlan& copy(original);
    EXPECT_EQ(copy.rootOperators.size(), 1);
    EXPECT_EQ(copy.rootOperators[0], sourceOp);
}

TEST_F(LogicalPlanTest, PromoteOperatorToRoot)
{
    auto sourceOp = SourceNameLogicalOperator("source");
    auto selectionOp = SelectionLogicalOperator(FieldAccessLogicalFunction("field"));
    auto plan = LogicalPlan(sourceOp);
    auto promoteResultPlan = promoteOperatorToRoot(plan, selectionOp);
    EXPECT_EQ(promoteResultPlan.rootOperators[0].getId(), selectionOp.id);
    EXPECT_EQ(promoteResultPlan.rootOperators[0].getChildren()[0].getId(), sourceOp.id);
}

TEST_F(LogicalPlanTest, ReplaceOperator)
{
    auto sourceOp = SourceNameLogicalOperator("source");
    auto sourceOp2 = SourceNameLogicalOperator("source2");
    auto plan = LogicalPlan(sourceOp);
    auto result = replaceOperator(plan, sourceOp, sourceOp2);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->rootOperators[0].getId(), sourceOp2.id);
}

TEST_F(LogicalPlanTest, replaceSubtree)
{
    auto sourceOp = SourceNameLogicalOperator("source");
    auto sourceOp2 = SourceNameLogicalOperator("source2");
    auto plan = LogicalPlan(sourceOp);
    auto result = replaceSubtree(plan, sourceOp, sourceOp2);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->rootOperators[0].getId(), sourceOp2.id);
}

TEST_F(LogicalPlanTest, GetParents)
{
    auto sourceOp = SourceNameLogicalOperator("source");
    auto selectionOp = SelectionLogicalOperator(FieldAccessLogicalFunction("field"));
    auto plan = LogicalPlan(sourceOp);
    auto promoteResult = promoteOperatorToRoot(plan, selectionOp);
    auto parents = getParents(promoteResult, sourceOp);
    EXPECT_EQ(parents.size(), 1);
    EXPECT_EQ(parents[0].getId(), selectionOp.id);
}

TEST_F(LogicalPlanTest, GetOperatorByType)
{
    auto sourceOp = SourceNameLogicalOperator("source");
    auto plan = LogicalPlan(sourceOp);
    auto sourceOperators = getOperatorByType<SourceNameLogicalOperator>(plan);
    EXPECT_EQ(sourceOperators.size(), 1);
    EXPECT_EQ(sourceOperators[0].id, sourceOp.id);
}

TEST_F(LogicalPlanTest, GetOperatorsByTraits)
{
    const LogicalPlan plan(sourceOp2);
    auto operators = getOperatorsByTraits<OriginIdAssignerTrait>(plan);
    EXPECT_EQ(operators.size(), 1);
    EXPECT_EQ(operators[0].getId(), sourceOp2.getId());
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
    auto allOperators = flatten(plan);
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
