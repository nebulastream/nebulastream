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

#include <gtest/gtest.h>
#include <LogicalPlans/Plan.hpp>
#include <LogicalOperators/Sources/SourceNameOperator.hpp>
#include <LogicalOperators/SelectionOperator.hpp>
#include <LogicalFunctions/FieldAccessFunction.hpp>
#include <LogicalOperators/Sources/SourceDescriptorOperator.hpp>
#include <Traits/OriginIdAssignerTrait.hpp>

using namespace NES;
using namespace NES::Logical;

class PlanTest : public ::testing::Test {
protected:
    void SetUp() override {
        /// Create some test operators
        sourceOp = SourceNameOperator("Source");
        auto dummySchema = Schema();
        auto dummyParserConfig = Sources::ParserConfig{"CSV", "\n", ","};
        auto dummySourceDescriptor = std::make_shared<Sources::SourceDescriptor>(
            dummySchema,
            "Source2",
            "CSV",
            Sources::SourceDescriptor::INVALID_NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL,
            dummyParserConfig,
            Configurations::DescriptorConfig::Config{});
        sourceOp2 = SourceDescriptorOperator(std::move(dummySourceDescriptor));
        selectionOp = SelectionOperator(FieldAccessFunction("logicalfunction"));
    }

    Operator sourceOp;
    Operator sourceOp2;
    Operator selectionOp;
};

TEST_F(PlanTest, DefaultConstructor) {
    Plan plan;
    EXPECT_TRUE(plan.rootOperators.empty());
    EXPECT_EQ(plan.queryId, INVALID_QUERY_ID);
}

TEST_F(PlanTest, SingleRootConstructor) {
    Plan plan(sourceOp);
    EXPECT_EQ(plan.rootOperators.size(), 1);
    EXPECT_EQ(plan.rootOperators[0], sourceOp);
}

TEST_F(PlanTest, MultipleRootsConstructor) {
    std::vector roots = {sourceOp, selectionOp};
    QueryId queryId = QueryId(1);
    Plan plan(queryId, roots);
    EXPECT_EQ(plan.rootOperators.size(), 2);
    EXPECT_EQ(plan.queryId, queryId);
    EXPECT_EQ(plan.rootOperators[0], sourceOp);
    EXPECT_EQ(plan.rootOperators[1], selectionOp);
}

TEST_F(PlanTest, CopyConstructor) {
    Plan original(sourceOp);
    Plan copy(original);
    EXPECT_EQ(copy.rootOperators.size(), 1);
    EXPECT_EQ(copy.rootOperators[0], sourceOp);
}

TEST_F(PlanTest, MoveConstructor) {
    Plan original(sourceOp);
    Plan moved(std::move(original));
    EXPECT_EQ(moved.rootOperators.size(), 1);
    EXPECT_EQ(moved.rootOperators[0], sourceOp);
    EXPECT_TRUE(original.rootOperators.empty());
}

TEST_F(PlanTest, MoveAssignment) {
    Plan original(sourceOp);
    Plan moved;
    moved = std::move(original);
    EXPECT_EQ(moved.rootOperators.size(), 1);
    EXPECT_EQ(moved.rootOperators[0], sourceOp);
    EXPECT_TRUE(original.rootOperators.empty());
}

TEST_F(PlanTest, PromoteOperatorToRoot) {
    Plan plan(sourceOp);
    plan.promoteOperatorToRoot(selectionOp);
    EXPECT_EQ(plan.rootOperators.size(), 1);
    EXPECT_EQ(plan.rootOperators[0], selectionOp);
}

TEST_F(PlanTest, ReplaceOperator) {
    Plan plan(sourceOp);
    std::vector children = {selectionOp};
    sourceOp = sourceOp.withChildren(children);
    EXPECT_TRUE(plan.replaceOperator(sourceOp, sourceOp2));
    EXPECT_EQ(plan.rootOperators[0], sourceOp2);
    /// Replace operator does not take the children
    EXPECT_EQ(sourceOp2.getChildren().size(), 0);
}

TEST_F(PlanTest, ReplaceOperatorExact) {
    Plan plan(sourceOp);
    std::vector children = {selectionOp};
    sourceOp = sourceOp.withChildren(children);
    sourceOp2 = sourceOp2.withChildren(children);
    EXPECT_TRUE(plan.replaceOperatorExact(sourceOp, sourceOp2));
    EXPECT_EQ(plan.rootOperators[0], sourceOp2);
    EXPECT_EQ(sourceOp2.getChildren().size(), 1);
    EXPECT_EQ(sourceOp2.getChildren()[0], selectionOp);
}

TEST_F(PlanTest, GetParents) {
    std::vector children = {selectionOp};
    sourceOp = sourceOp.withChildren(children);
    Plan plan(sourceOp);
    auto parents = plan.getParents(selectionOp);
    EXPECT_EQ(parents.size(), 1);
    EXPECT_EQ(parents[0], sourceOp);
}

TEST_F(PlanTest, GetOperatorByType) {
    Plan plan(sourceOp);
    auto sourceOperators = plan.getOperatorByType<SourceNameOperator>();
    EXPECT_EQ(sourceOperators.size(), 1);
    EXPECT_EQ(sourceOperators[0], sourceOp);
}

TEST_F(PlanTest, GetOperatorsByTraits) {
    Plan plan(sourceOp2);
    auto operators = plan.getOperatorsByTraits<NES::Optimizer::OriginIdAssignerTrait>();
    EXPECT_EQ(operators.size(), 1);
    EXPECT_EQ(operators[0], sourceOp2);
}

TEST_F(PlanTest, GetLeafOperators) {
    /// source1 -> selection -> source2
    std::vector children = {sourceOp2};
    selectionOp = selectionOp.withChildren(children);
    children = {selectionOp};
    sourceOp = sourceOp.withChildren(children);
    Plan plan(sourceOp);
    auto leafOperators = plan.getLeafOperators();
    EXPECT_EQ(leafOperators.size(), 1);
    EXPECT_EQ(leafOperators[0], sourceOp2);
}

TEST_F(PlanTest, GetAllOperators) {
    /// source1 -> selection -> source2
    std::vector children = {sourceOp2};
    selectionOp = selectionOp.withChildren(children);
    children = {selectionOp};
    sourceOp = sourceOp.withChildren(children);
    Plan plan(sourceOp);
    auto allOperators = plan.getAllOperators();
    EXPECT_EQ(allOperators.size(), 3);
    EXPECT_TRUE(allOperators.contains(sourceOp));
    EXPECT_TRUE(allOperators.contains(selectionOp));
    EXPECT_TRUE(allOperators.contains(sourceOp2));
}

TEST_F(PlanTest, EqualityOperator) {
    Plan plan1(sourceOp);
    Plan plan2(sourceOp);
    EXPECT_TRUE(plan1 == plan2);
    
    Plan plan3(selectionOp);
    EXPECT_FALSE(plan1 == plan3);
}

TEST_F(PlanTest, OutputOperator) {
    Plan plan(sourceOp);
    std::stringstream ss;
    ss << plan;
    EXPECT_FALSE(ss.str().empty());
} 