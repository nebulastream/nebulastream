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
#include <sstream>
#include <string>
#include <vector>

#include <Identifiers/NESStrongType.hpp>
#include <Util/PlanRenderer.hpp>
#include <gtest/gtest.h>

namespace NES
{

/// Lightweight mock operator for testing PlanRenderer without depending on nes-logical-operators.
struct MockOperator
{
    using IdType = NESStrongType<uint64_t, struct MockOperatorId_, 0, 1>;

    std::string label;
    IdType opId;
    std::vector<MockOperator> children;

    [[nodiscard]] std::string explain(ExplainVerbosity) const { return label; }

    [[nodiscard]] IdType getId() const { return opId; }

    [[nodiscard]] std::vector<MockOperator> getChildren() const { return children; }
};

/// Lightweight mock plan that holds root operators.
struct MockPlan
{
    std::vector<MockOperator> roots;

    [[nodiscard]] std::vector<MockOperator> getRootOperators() const { return roots; }
};

class PlanRendererTest : public ::testing::Test
{
};

/// Linear chain: SINK -> FILTER -> MAP -> SOURCE
TEST_F(PlanRendererTest, printQuerySourceFilterMapSink)
{
    const MockOperator source{.label = "SOURCE(stream)", .opId = MockOperator::IdType(1), .children = {}};
    const MockOperator map{.label = "MAP(x * 2)", .opId = MockOperator::IdType(2), .children = {source}};
    const MockOperator filter{.label = "FILTER(x > 10)", .opId = MockOperator::IdType(3), .children = {map}};
    const MockOperator sink{.label = "SINK(output)", .opId = MockOperator::IdType(4), .children = {filter}};

    const MockPlan plan{.roots = {sink}};

    std::ostringstream oss;
    PlanRenderer<MockPlan, MockOperator> renderer(oss, ExplainVerbosity::Short);
    renderer.dump(plan);

    const auto output = oss.str();
    /// All nodes should appear in the output.
    EXPECT_NE(output.find("SINK(output)"), std::string::npos);
    EXPECT_NE(output.find("FILTER(x > 10)"), std::string::npos);
    EXPECT_NE(output.find("MAP(x * 2)"), std::string::npos);
    EXPECT_NE(output.find("SOURCE(stream)"), std::string::npos);

    /// SINK should appear before SOURCE (top-down rendering).
    EXPECT_LT(output.find("SINK(output)"), output.find("SOURCE(stream)"));
}

/// DAG with two sinks sharing a common source.
TEST_F(PlanRendererTest, printQueryMapFilterTwoSinks)
{
    const MockOperator source{.label = "SOURCE(stream)", .opId = MockOperator::IdType(1), .children = {}};
    const MockOperator filter{.label = "FILTER(x > 5)", .opId = MockOperator::IdType(2), .children = {source}};
    const MockOperator map{.label = "MAP(x + 1)", .opId = MockOperator::IdType(3), .children = {source}};
    const MockOperator sink1{.label = "SINK(out1)", .opId = MockOperator::IdType(4), .children = {filter}};
    const MockOperator sink2{.label = "SINK(out2)", .opId = MockOperator::IdType(5), .children = {map}};

    const MockPlan plan{.roots = {sink1, sink2}};

    std::ostringstream oss;
    PlanRenderer<MockPlan, MockOperator> renderer(oss, ExplainVerbosity::Short);
    renderer.dump(plan);

    const auto output = oss.str();
    /// All nodes present.
    EXPECT_NE(output.find("SINK(out1)"), std::string::npos);
    EXPECT_NE(output.find("SINK(out2)"), std::string::npos);
    EXPECT_NE(output.find("FILTER(x > 5)"), std::string::npos);
    EXPECT_NE(output.find("MAP(x + 1)"), std::string::npos);
    EXPECT_NE(output.find("SOURCE(stream)"), std::string::npos);

    /// Source should appear exactly once despite having two parents.
    const auto firstPos = output.find("SOURCE(stream)");
    const auto secondPos = output.find("SOURCE(stream)", firstPos + 1);
    EXPECT_EQ(secondPos, std::string::npos) << "SOURCE should appear only once in the DAG rendering";
}

/// Binary tree: JOIN with two children.
TEST_F(PlanRendererTest, printJoinWithTwoChildren)
{
    const MockOperator left{.label = "SOURCE(left)", .opId = MockOperator::IdType(1), .children = {}};
    const MockOperator right{.label = "SOURCE(right)", .opId = MockOperator::IdType(2), .children = {}};
    const MockOperator join{.label = "JOIN(id = id)", .opId = MockOperator::IdType(3), .children = {left, right}};
    const MockOperator sink{.label = "SINK(result)", .opId = MockOperator::IdType(4), .children = {join}};

    const MockPlan plan{.roots = {sink}};

    std::ostringstream oss;
    PlanRenderer<MockPlan, MockOperator> renderer(oss, ExplainVerbosity::Short);
    renderer.dump(plan);

    const auto output = oss.str();
    EXPECT_NE(output.find("SINK(result)"), std::string::npos);
    EXPECT_NE(output.find("JOIN(id = id)"), std::string::npos);
    EXPECT_NE(output.find("SOURCE(left)"), std::string::npos);
    EXPECT_NE(output.find("SOURCE(right)"), std::string::npos);

    /// Branch connectors should be present (Unicode box drawing).
    EXPECT_NE(output.find("\xe2\x94\x8c"), std::string::npos) << "Expected branch connector in output"; /// ┌
}

/// Single node: just a source, no children.
TEST_F(PlanRendererTest, printSingleNode)
{
    const MockOperator source{.label = "SOURCE(stream)", .opId = MockOperator::IdType(1), .children = {}};
    const MockPlan plan{.roots = {source}};

    std::ostringstream oss;
    PlanRenderer<MockPlan, MockOperator> renderer(oss, ExplainVerbosity::Short);
    renderer.dump(plan);

    const auto output = oss.str();
    EXPECT_NE(output.find("SOURCE(stream)"), std::string::npos);
}

/// Long label gets truncated.
TEST_F(PlanRendererTest, longLabelGetsTruncated)
{
    /// Label exceeds MAX_NODE_DISPLAY_WIDTH (60 chars).
    constexpr size_t labelLength = MAX_NODE_DISPLAY_WIDTH + 40;
    const std::string longLabel(labelLength, 'X');
    const MockOperator node{.label = longLabel, .opId = MockOperator::IdType(1), .children = {}};
    const MockPlan plan{.roots = {node}};

    std::ostringstream oss;
    PlanRenderer<MockPlan, MockOperator> renderer(oss, ExplainVerbosity::Short);
    renderer.dump(plan);

    const auto output = oss.str();
    /// The full label should NOT appear.
    EXPECT_EQ(output.find(longLabel), std::string::npos);
    /// The truncated version with "..." should appear.
    EXPECT_NE(output.find("..."), std::string::npos);
}

}
