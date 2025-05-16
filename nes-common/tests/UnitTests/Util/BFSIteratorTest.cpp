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
#include <iterator>
#include <utility>
#include <vector>
#include <Iterators/BFSIterator.hpp>
#include <gtest/gtest.h>

namespace NES
{

class TestNode
{
public:
    explicit TestNode(uint64_t id) : id(id) { }
    explicit TestNode(uint64_t id, std::vector<TestNode> children) : id(id), children(std::move(children)) { }

    [[nodiscard]] uint64_t getId() const { return id; }

    [[nodiscard]] const std::vector<TestNode>& getChildren() const { return children; }

    bool operator==(const TestNode& other) const { return id == other.id; }

private:
    uint64_t id;
    const std::vector<TestNode> children;
};

TEST(BFSIteratorTest, BasicTraversal)
{
    const TestNode node4(4);
    const TestNode node5(5);
    const TestNode node6(6);
    const TestNode node2(2, {node4, node5});
    const TestNode node3(3, {node6});
    const TestNode root(1, {node2, node3});

    std::vector<uint64_t> visitedIds;
    for (const auto& node : BFSRange(root))
    {
        visitedIds.push_back(node.getId());
    }

    const std::vector<uint64_t> expectedOrder = {1, 2, 3, 4, 5, 6};
    EXPECT_EQ(visitedIds, expectedOrder);
}

TEST(BFSIteratorTest, EmptyTree)
{
    const TestNode root(1);
    std::vector<uint64_t> visitedIds;
    for (const auto& node : BFSRange(root))
    {
        visitedIds.push_back(node.getId());
    }

    const std::vector<uint64_t> expectedOrder = {1};
    EXPECT_EQ(visitedIds, expectedOrder);
}

TEST(BFSIteratorTest, IteratorEquality)
{
    const TestNode node4(4);
    const TestNode node5(5);
    const TestNode node6(6);
    const TestNode node2(2, {node4, node5});
    const TestNode node3(3, {node6});
    const TestNode root(1, {node2, node3});

    const TestNode otherRoot(7, {node2, node3});

    const BFSRange expectedRange(root);
    const BFSRange otherRange(otherRoot);

    auto expIt1 = expectedRange.begin();
    auto expIt2 = expectedRange.begin();
    const auto otherIt = otherRange.begin();

    EXPECT_EQ(expIt1, expIt2);
    EXPECT_NE(expIt1, otherIt);

    const auto expIt1Next = std::next(expIt1);
    EXPECT_NE(expIt1, expIt1Next);
    EXPECT_EQ(expIt1Next, ++expIt2);
}

}
