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
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Topology/Prediction/Edge.hpp>
#include <Catalogs/Topology/Prediction/TopologyChangeLog.hpp>
#include <Catalogs/Topology/Prediction/TopologyDelta.hpp>
#include <gtest/gtest.h>

namespace NES {
using Experimental::TopologyPrediction::Edge;
using Experimental::TopologyPrediction::TopologyChangeLog;
using Experimental::TopologyPrediction::TopologyDelta;

class TopologyChangeLogTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TopologyChangelogTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Set up TopologyChangelog test class");
    }
};

TEST_F(TopologyChangeLogTest, testEmptyChangeLog) {
    TopologyChangeLog emptyChangelog;
    ASSERT_TRUE(emptyChangelog.empty());
    TopologyDelta delta1({{1, 2}, {1, 3}, {5, 3}}, {{2, 3}, {1, 5}});
    TopologyChangeLog nonEmptyChangeLog;
    nonEmptyChangeLog.update(delta1);
    ASSERT_FALSE(nonEmptyChangeLog.empty());
}

TEST_F(TopologyChangeLogTest, testInsertingDelta) {
    TopologyDelta delta1({{1, 2}, {1, 3}, {5, 3}}, {{2, 3}, {1, 5}});
    TopologyChangeLog log1;
    log1.update(delta1);

    auto addedChildren2 = log1.getAddedChildren(2);
    EXPECT_EQ(addedChildren2.size(), 1);
    EXPECT_NE(std::find(addedChildren2.begin(), addedChildren2.end(), 1), addedChildren2.end());
    auto addedChildren3 = log1.getAddedChildren(3);
    EXPECT_EQ(addedChildren3.size(), 2);
    EXPECT_NE(std::find(addedChildren3.begin(), addedChildren3.end(), 1), addedChildren3.end());
    EXPECT_NE(std::find(addedChildren3.begin(), addedChildren3.end(), 5), addedChildren3.end());
    auto addedChildren4 = log1.getAddedChildren(4);
    EXPECT_TRUE(addedChildren4.empty());
    auto addedChildren5 = log1.getAddedChildren(5);
    EXPECT_TRUE(addedChildren5.empty());

    auto removedChildren2 = log1.getRemovedChildren(2);
    EXPECT_TRUE(removedChildren2.empty());
    auto removedChildren3 = log1.getRemovedChildren(3);
    EXPECT_EQ(removedChildren3.size(), 1);
    EXPECT_NE(std::find(removedChildren3.begin(), removedChildren3.end(), 2), removedChildren3.end());
    auto removedChildren4 = log1.getRemovedChildren(4);
    EXPECT_TRUE(removedChildren4.empty());
    auto removedChildren5 = log1.getRemovedChildren(5);
    EXPECT_EQ(removedChildren5.size(), 1);
    EXPECT_NE(std::find(removedChildren5.begin(), removedChildren5.end(), 1), removedChildren5.end());
}

TEST_F(TopologyChangeLogTest, testErasing) {
    TopologyDelta delta1({{1, 2}, {1, 3}, {5, 3}, {6, 3}, {7, 3}, {1, 4}}, {{2, 3}, {1, 5}, {2, 5}, {3, 5}, {8, 4}});
    TopologyDelta delta2({{6, 3}, {7, 3}, {1, 4}}, {{2, 5}, {3, 5}, {8, 4}});
    TopologyChangeLog log1;
    log1.update(delta1);
    log1.erase(delta2);

    auto addedChildren2 = log1.getAddedChildren(2);
    EXPECT_EQ(addedChildren2.size(), 1);
    EXPECT_NE(std::find(addedChildren2.begin(), addedChildren2.end(), 1), addedChildren2.end());
    auto addedChildren3 = log1.getAddedChildren(3);
    EXPECT_EQ(addedChildren3.size(), 2);
    EXPECT_NE(std::find(addedChildren3.begin(), addedChildren3.end(), 1), addedChildren3.end());
    EXPECT_NE(std::find(addedChildren3.begin(), addedChildren3.end(), 5), addedChildren3.end());
    auto addedChildren4 = log1.getAddedChildren(4);
    EXPECT_TRUE(addedChildren4.empty());
    auto addedChildren5 = log1.getAddedChildren(5);
    EXPECT_TRUE(addedChildren5.empty());

    auto removedChildren2 = log1.getRemovedChildren(2);
    EXPECT_TRUE(removedChildren2.empty());
    auto removedChildren3 = log1.getRemovedChildren(3);
    EXPECT_EQ(removedChildren3.size(), 1);
    EXPECT_NE(std::find(removedChildren3.begin(), removedChildren3.end(), 2), removedChildren3.end());
    auto removedChildren4 = log1.getRemovedChildren(4);
    EXPECT_TRUE(removedChildren4.empty());
    auto removedChildren5 = log1.getRemovedChildren(5);
    EXPECT_EQ(removedChildren5.size(), 1);
    EXPECT_NE(std::find(removedChildren5.begin(), removedChildren5.end(), 1), removedChildren5.end());
}

TEST_F(TopologyChangeLogTest, testAddingChangeLog) {
    TopologyDelta delta1({{1, 2}, {1, 3}, {5, 3}}, {{2, 3}, {1, 5}});
    TopologyDelta delta2({{2, 3}, {2, 4}}, {{1, 2}, {2, 5}, {7, 5}});

    TopologyChangeLog log1;
    TopologyChangeLog log2;
    log1.update(delta1);
    log2.update(delta2);
    log1.add(log2);

    auto addedChildren2 = log1.getAddedChildren(2);
    EXPECT_TRUE(addedChildren2.empty());
    auto addedChildren3 = log1.getAddedChildren(3);
    EXPECT_EQ(addedChildren3.size(), 2);
    EXPECT_NE(std::find(addedChildren3.begin(), addedChildren3.end(), 1), addedChildren3.end());
    EXPECT_NE(std::find(addedChildren3.begin(), addedChildren3.end(), 5), addedChildren3.end());
    auto addedChildren4 = log1.getAddedChildren(4);
    EXPECT_EQ(addedChildren4.size(), 1);
    EXPECT_NE(std::find(addedChildren4.begin(), addedChildren4.end(), 2), addedChildren4.end());
    auto addedChildren5 = log1.getAddedChildren(5);
    EXPECT_TRUE(addedChildren5.empty());

    auto removedChildren2 = log1.getRemovedChildren(2);
    EXPECT_TRUE(removedChildren2.empty());
    auto removedChildren3 = log1.getRemovedChildren(3);
    EXPECT_TRUE(removedChildren3.empty());
    auto removedChildren4 = log1.getRemovedChildren(4);
    EXPECT_TRUE(removedChildren4.empty());
    auto removedChildren5 = log1.getRemovedChildren(5);
    EXPECT_EQ(removedChildren5.size(), 3);
    EXPECT_NE(std::find(removedChildren5.begin(), removedChildren5.end(), 1), removedChildren5.end());
    EXPECT_NE(std::find(removedChildren5.begin(), removedChildren5.end(), 2), removedChildren5.end());
    EXPECT_NE(std::find(removedChildren5.begin(), removedChildren5.end(), 7), removedChildren5.end());
}

TEST_F(TopologyChangeLogTest, testRemovingInexistentEdge) {
    TopologyDelta delta1({{1, 2}, {1, 3}, {5, 3}, {6, 3}, {7, 3}, {1, 4}}, {{2, 3}, {1, 5}, {2, 5}, {3, 5}, {8, 4}});
    TopologyDelta delta2({{6, 4}}, {});
    TopologyChangeLog log1;
    log1.update(delta1);
    EXPECT_THROW(log1.erase(delta2), Exceptions::RuntimeException);
}

TEST_F(TopologyChangeLogTest, testRemovingEmptyDelta) {
    TopologyDelta delta1({{1, 2}}, {{2, 3}});
    TopologyDelta delta2({{}}, {});
    TopologyChangeLog log1;
    log1.update(delta1);
    log1.erase(delta2);

    EXPECT_EQ(log1.getAddedChildren(2), std::vector<uint64_t>{1});
    EXPECT_EQ(log1.getRemovedChildren(3), std::vector<uint64_t>{2});
}

TEST_F(TopologyChangeLogTest, testAddingEmptyDelta) {
    TopologyDelta delta1({{1, 2}}, {{2, 3}});
    TopologyDelta delta2({{}}, {});
    TopologyChangeLog log1;
    log1.update(delta1);
    log1.update(delta2);

    EXPECT_EQ(log1.getAddedChildren(2), std::vector<uint64_t>{1});
    EXPECT_EQ(log1.getRemovedChildren(3), std::vector<uint64_t>{2});
}

TEST_F(TopologyChangeLogTest, testInsertingEmptyChangelog) {
    TopologyDelta delta1({{1, 2}}, {{2, 3}});
    TopologyDelta delta2({{}}, {});
    TopologyChangeLog log1;
    TopologyChangeLog log2;
    log1.update(delta1);
    log2.update(delta2);
    log1.add(log2);

    EXPECT_EQ(log1.getAddedChildren(2), std::vector<uint64_t>{1});
    EXPECT_EQ(log1.getRemovedChildren(3), std::vector<uint64_t>{2});
}
}// namespace NES
