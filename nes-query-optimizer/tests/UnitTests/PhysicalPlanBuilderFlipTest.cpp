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
#include <memory>
#include <queue>
#include <unordered_set>
#include <vector>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <PhysicalOperator.hpp>
#include <PhysicalPlan.hpp>
#include <PhysicalPlanBuilder.hpp>
#include <SinkPhysicalOperator.hpp>
#include <SourcePhysicalOperator.hpp>
#include <UnionPhysicalOperator.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>

namespace NES
{
namespace
{

using PipelineLocation = PhysicalOperatorWrapper::PipelineLocation;

class PhysicalPlanBuilderFlipTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("PhysicalPlanBuilderFlipTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override { BaseUnitTest::SetUp(); }

    static Schema createSchema()
    {
        Schema schema;
        schema.addField("test.id", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        schema.addField("test.value", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        return schema;
    }

    /// Creates a PhysicalOperatorWrapper holding a SourcePhysicalOperator.
    std::shared_ptr<PhysicalOperatorWrapper> makeSourceWrapper()
    {
        auto schema = createSchema();
        auto descriptor = sourceCatalog.getInlineSource("File", schema, {{"type", "CSV"}}, {{"file_path", "/dev/null"}});
        EXPECT_TRUE(descriptor.has_value());
        auto sourceOp = SourcePhysicalOperator(std::move(descriptor.value()), OriginId(nextOriginId++));
        return std::make_shared<PhysicalOperatorWrapper>(
            PhysicalOperator{sourceOp}, schema, schema, MemoryLayoutType::ROW_LAYOUT, MemoryLayoutType::ROW_LAYOUT, PipelineLocation::SCAN);
    }

    /// Creates a PhysicalOperatorWrapper holding a SinkPhysicalOperator.
    std::shared_ptr<PhysicalOperatorWrapper> makeSinkWrapper()
    {
        auto schema = createSchema();
        auto descriptor = sinkCatalog.getInlineSink(schema, "Print", {{"input_format", "CSV"}});
        EXPECT_TRUE(descriptor.has_value());
        auto sinkOp = SinkPhysicalOperator(descriptor.value());
        return std::make_shared<PhysicalOperatorWrapper>(
            PhysicalOperator{sinkOp}, schema, schema, MemoryLayoutType::ROW_LAYOUT, MemoryLayoutType::ROW_LAYOUT, PipelineLocation::EMIT);
    }

    /// Creates a PhysicalOperatorWrapper holding a UnionPhysicalOperator.
    static std::shared_ptr<PhysicalOperatorWrapper> makeUnionWrapper()
    {
        auto schema = createSchema();
        auto unionOp = UnionPhysicalOperator();
        return std::make_shared<PhysicalOperatorWrapper>(
            PhysicalOperator{unionOp},
            schema,
            schema,
            MemoryLayoutType::ROW_LAYOUT,
            MemoryLayoutType::ROW_LAYOUT,
            PipelineLocation::INTERMEDIATE);
    }

    /// Counts total edges (parent→child links) in a plan by BFS from roots.
    static size_t countEdges(const PhysicalPlan& plan)
    {
        size_t edges = 0;
        std::queue<std::shared_ptr<PhysicalOperatorWrapper>> queue;
        std::unordered_set<PhysicalOperatorWrapper*> visited;
        for (const auto& root : plan.getRootOperators())
        {
            queue.push(root);
            visited.insert(root.get());
        }
        while (!queue.empty())
        {
            auto current = queue.front();
            queue.pop();
            for (const auto& child : current->getChildren())
            {
                ++edges;
                if (visited.insert(child.get()).second)
                {
                    queue.push(child);
                }
            }
        }
        return edges;
    }

    /// Counts total nodes reachable from roots by BFS.
    static size_t countNodes(const PhysicalPlan& plan)
    {
        std::queue<std::shared_ptr<PhysicalOperatorWrapper>> queue;
        std::unordered_set<PhysicalOperatorWrapper*> visited;
        for (const auto& root : plan.getRootOperators())
        {
            queue.push(root);
            visited.insert(root.get());
        }
        while (!queue.empty())
        {
            auto current = queue.front();
            queue.pop();
            for (const auto& child : current->getChildren())
            {
                if (visited.insert(child.get()).second)
                {
                    queue.push(child);
                }
            }
        }
        return visited.size();
    }

    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;
    uint64_t nextOriginId = 1;
};

/// Sink → Source. After finalize(), verify source is root with sink as descendant.
TEST_F(PhysicalPlanBuilderFlipTest, LinearChainFlip)
{
    auto source = makeSourceWrapper();
    auto sink = makeSinkWrapper();

    /// Build sink→source graph: sink is root, source is child.
    sink->addChild(source);

    auto builder = PhysicalPlanBuilder(QueryId(1));
    builder.addSinkRoot(sink);
    auto plan = std::move(builder).finalize();

    /// After flip, source should be root.
    ASSERT_EQ(plan.getRootOperators().size(), 1u);
    const auto& root = plan.getRootOperators()[0];
    EXPECT_TRUE(root->getPhysicalOperator().tryGet<SourcePhysicalOperator>());

    /// Root's child should be the sink.
    ASSERT_EQ(root->getChildren().size(), 1u);
    EXPECT_TRUE(root->getChildren()[0]->getPhysicalOperator().tryGet<SinkPhysicalOperator>());

    /// Sink (now a leaf) should have no children.
    EXPECT_TRUE(root->getChildren()[0]->getChildren().empty());
}

/// Sink → Union → [Source1, Source2]. Verify two source roots, both eventually reach sink.
TEST_F(PhysicalPlanBuilderFlipTest, DiamondShapeFlip)
{
    auto source1 = makeSourceWrapper();
    auto source2 = makeSourceWrapper();
    auto unionOp = makeUnionWrapper();
    auto sink = makeSinkWrapper();

    /// Build sink→source graph: sink → union → {source1, source2}.
    unionOp->addChild(source1);
    unionOp->addChild(source2);
    sink->addChild(unionOp);

    auto builder = PhysicalPlanBuilder(QueryId(2));
    builder.addSinkRoot(sink);
    auto plan = std::move(builder).finalize();

    /// After flip, there should be 2 source roots.
    ASSERT_EQ(plan.getRootOperators().size(), 2u);
    for (const auto& root : plan.getRootOperators())
    {
        EXPECT_TRUE(root->getPhysicalOperator().tryGet<SourcePhysicalOperator>());
    }

    /// All 4 nodes must be reachable.
    EXPECT_EQ(countNodes(plan), 4u);

    /// Both source roots should reach the sink (via union).
    for (const auto& root : plan.getRootOperators())
    {
        ASSERT_EQ(root->getChildren().size(), 1u);
        auto child = root->getChildren()[0];
        EXPECT_TRUE(child->getPhysicalOperator().tryGet<UnionPhysicalOperator>());
        ASSERT_EQ(child->getChildren().size(), 1u);
        EXPECT_TRUE(child->getChildren()[0]->getPhysicalOperator().tryGet<SinkPhysicalOperator>());
    }
}

/// Sink → Union1 → Union2 → Source. Verify correct depth after flip.
TEST_F(PhysicalPlanBuilderFlipTest, MultiOperatorChainFlip)
{
    auto source = makeSourceWrapper();
    auto union1 = makeUnionWrapper();
    auto union2 = makeUnionWrapper();
    auto sink = makeSinkWrapper();

    /// Build sink→source chain: sink → union1 → union2 → source.
    union2->addChild(source);
    union1->addChild(union2);
    sink->addChild(union1);

    auto builder = PhysicalPlanBuilder(QueryId(3));
    builder.addSinkRoot(sink);
    auto plan = std::move(builder).finalize();

    /// After flip: source → union2 → union1 → sink.
    ASSERT_EQ(plan.getRootOperators().size(), 1u);
    auto current = plan.getRootOperators()[0];
    EXPECT_TRUE(current->getPhysicalOperator().tryGet<SourcePhysicalOperator>());

    ASSERT_EQ(current->getChildren().size(), 1u);
    current = current->getChildren()[0];
    EXPECT_TRUE(current->getPhysicalOperator().tryGet<UnionPhysicalOperator>());

    ASSERT_EQ(current->getChildren().size(), 1u);
    current = current->getChildren()[0];
    EXPECT_TRUE(current->getPhysicalOperator().tryGet<UnionPhysicalOperator>());

    ASSERT_EQ(current->getChildren().size(), 1u);
    current = current->getChildren()[0];
    EXPECT_TRUE(current->getPhysicalOperator().tryGet<SinkPhysicalOperator>());

    EXPECT_TRUE(current->getChildren().empty());
}

/// Build a graph, count edges before finalize, verify same count in the resulting PhysicalPlan.
TEST_F(PhysicalPlanBuilderFlipTest, EdgeCountPreserved)
{
    auto source1 = makeSourceWrapper();
    auto source2 = makeSourceWrapper();
    auto unionOp = makeUnionWrapper();
    auto sink = makeSinkWrapper();

    /// Sink → Union → {Source1, Source2}: 3 edges total.
    unionOp->addChild(source1);
    unionOp->addChild(source2);
    sink->addChild(unionOp);

    const size_t expectedEdges = 3;

    auto builder = PhysicalPlanBuilder(QueryId(4));
    builder.addSinkRoot(sink);
    auto plan = std::move(builder).finalize();

    EXPECT_EQ(countEdges(plan), expectedEdges);
}

}
}
