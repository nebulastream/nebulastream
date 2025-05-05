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
#include <Iterators/BFSIterator.hpp>
#include <vector>
#include <string>

namespace NES {

class TestNode {
public:
    explicit TestNode(int id, std::vector<TestNode> children = {}) : id(id), children(std::move(children)) {}

    int getId() const
    {
        return id;
    }

    const std::vector<TestNode>& getChildren() const {
        return children;
    }

    bool operator==(const TestNode& other) const {
        return id == other.id;
    }

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
    for (const auto& node : BFSRange(root)) {
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

TEST(BFSIteratorTest, IteratorEquality2)
{
    std::vector expectedOrder = {1, 2, 3, 4, 5, 6};
    std::vector otherOrder = {1, 2, 3, 4, 5, 6};
    const auto expVecIt1 = expectedOrder.begin();
    const auto expVecIt2 = expectedOrder.begin();
    const auto otherVecIt = otherOrder.begin();
    EXPECT_EQ(expVecIt1, expVecIt2);
    EXPECT_NE(expVecIt1, otherVecIt);
    EXPECT_NE((expVecIt1), (expVecIt1 + 1));
    EXPECT_EQ((expVecIt1 + 1), (expVecIt2 + 1));
}

}