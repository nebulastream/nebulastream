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
#include <BaseUnitTest.hpp>

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

/// Shared node re-parented from a parent on its own layer.
/// `shared` is first placed on a layer via `branchA`, then referenced again by `mid`, which sits on that same layer. This drives
/// `insertVerticalBranches` with `startDepth == endDepth`, which previously left the original node in place while also re-queuing it,
/// making it render twice.
TEST_F(PlanRendererTest, sharedNodeReParentedFromSameLayerRendersOnce)
{
    const MockOperator shared{.label = "SHARED(node)", .opId = MockOperator::IdType(1), .children = {}};
    const MockOperator branchA{.label = "BRANCH(a)", .opId = MockOperator::IdType(2), .children = {shared}};
    const MockOperator mid{.label = "MID(b)", .opId = MockOperator::IdType(3), .children = {shared}};
    const MockOperator branchB{.label = "BRANCH(b)", .opId = MockOperator::IdType(4), .children = {mid}};
    const MockOperator sink{.label = "SINK(out)", .opId = MockOperator::IdType(5), .children = {branchA, branchB}};

    const MockPlan plan{.roots = {sink}};

    std::ostringstream oss;
    PlanRenderer<MockPlan, MockOperator> renderer(oss, ExplainVerbosity::Short);
    renderer.dump(plan);

    const auto output = oss.str();
    EXPECT_NE(output.find("SHARED(node)"), std::string::npos);
    EXPECT_NE(output.find("BRANCH(a)"), std::string::npos);
    EXPECT_NE(output.find("MID(b)"), std::string::npos);

    /// The shared node must appear exactly once despite being reachable from two parents on different layers.
    const auto firstPos = output.find("SHARED(node)");
    const auto secondPos = output.find("SHARED(node)", firstPos + 1);
    EXPECT_EQ(secondPos, std::string::npos) << "SHARED node should appear only once in the DAG rendering";
}

/// Deep DAG with several layers between a shared node and its deepest parent.
/// The shared node is placed early, then re-parented from a much deeper parent, forcing placeholder branches into already-finalised
/// layers. Previously this widened those layers without bumping their width, so a node line could exceed the computed maximum width
/// (and, on the widest layer, `drawTree` could place branches past the end of the branch line).
TEST_F(PlanRendererTest, deepDagWithSharedNodeDoesNotCrash)
{
    const MockOperator shared{.label = "SHARED(leaf)", .opId = MockOperator::IdType(1), .children = {}};
    /// A chain deep enough to put >= 3 layers between `shared` and `deep`.
    const MockOperator deep{.label = "DEEP(v)", .opId = MockOperator::IdType(2), .children = {shared}};
    const MockOperator l4{.label = "L4(w)", .opId = MockOperator::IdType(3), .children = {deep}};
    const MockOperator l3{.label = "L3(z)", .opId = MockOperator::IdType(4), .children = {l4}};
    const MockOperator l2{.label = "L2(y)", .opId = MockOperator::IdType(5), .children = {l3}};
    /// A shallow parent that places `shared` early on a shallow layer.
    const MockOperator shallow{.label = "SHALLOW(a)", .opId = MockOperator::IdType(6), .children = {shared}};
    const MockOperator sink{.label = "SINK(out)", .opId = MockOperator::IdType(7), .children = {shallow, l2}};

    const MockPlan plan{.roots = {sink}};

    std::ostringstream oss;
    PlanRenderer<MockPlan, MockOperator> renderer(oss, ExplainVerbosity::Short);
    renderer.dump(plan);

    const auto output = oss.str();
    EXPECT_NE(output.find("SHARED(leaf)"), std::string::npos);
    EXPECT_NE(output.find("DEEP(v)"), std::string::npos);

    /// Still rendered exactly once.
    const auto firstPos = output.find("SHARED(leaf)");
    const auto secondPos = output.find("SHARED(leaf)", firstPos + 1);
    EXPECT_EQ(secondPos, std::string::npos) << "SHARED node should appear only once in the deep DAG rendering";

    /// The first line is an empty branch line of exactly the maximum width. Every node line (odd lines, plain ASCII) must fit into it.
    std::istringstream lines(output);
    std::string firstLine;
    ASSERT_TRUE(std::getline(lines, firstLine));
    size_t lineIdx = 1;
    for (std::string line; std::getline(lines, line); ++lineIdx)
    {
        if (lineIdx % 2 == 1)
        {
            EXPECT_LE(line.size(), firstLine.size()) << "node line " << lineIdx << " exceeds the maximum width:\n" << output;
        }
    }
}

/// DAG (edges) 1->2, 1->4, 2->3, 3->4, 4->5. Node 4 is reachable both directly from node 1 and via 1->2->3, so
/// `insertVerticalBranches` re-parents it while node 5 is still queued holding a `weak_ptr` to the original (about to be destroyed)
/// node 4. `calculateLayers` used to lock that expired `weak_ptr` unconditionally and dereference null, crashing the whole process
/// (UB, not caught by `dump()`'s `CPPTRACE_CATCH`).
///
/// With the guard, asserts-disabled builds skip the stale edge and rendering completes; asserts-enabled builds still fail loudly via a
/// controlled `INVARIANT`. `EXPECT_DEATH_DEBUG` covers both: a death test with asserts, a plain "must not crash" check without.
TEST_F(PlanRendererTest, sharedNodeReparentedWhileChildQueuedDoesNotCrash)
{
    SKIP_IF_TSAN();

    const MockOperator node5{.label = "NODE_5", .opId = MockOperator::IdType(5), .children = {}};
    const MockOperator node4{.label = "NODE_4", .opId = MockOperator::IdType(4), .children = {node5}};
    const MockOperator node3{.label = "NODE_3", .opId = MockOperator::IdType(3), .children = {node4}};
    const MockOperator node2{.label = "NODE_2", .opId = MockOperator::IdType(2), .children = {node3}};
    const MockOperator node1{.label = "NODE_1", .opId = MockOperator::IdType(1), .children = {node2, node4}};

    const MockPlan plan{.roots = {node1}};

    /// A type alias avoids an unparenthesized comma (from the template argument list) inside the macro invocation below.
    using Renderer = PlanRenderer<MockPlan, MockOperator>;
    EXPECT_DEATH_DEBUG(
        {
            std::ostringstream oss;
            Renderer renderer(oss, ExplainVerbosity::Short);
            renderer.dump(plan);
        },
        "");
}

}
