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
#include <ostream>
#include <sstream>
#include <utility>
#include <vector>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Operator.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/LogicalQueryDumpHelper.hpp>
#include <gtest/gtest.h>
#include <sys/types.h>
#include <BaseUnitTest.hpp>

namespace NES
{
class LogicalQueryDumpHelperTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        NES::Logger::setupLogging("LogicalQueryDumpHelper.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LogicalQueryDumpHelper test class.");
    }

    void SetUp() override { Testing::BaseUnitTest::SetUp(); }

protected:
    u_int64_t milliseconds = 5000;
    const std::string leaf1 = "leaf1 (source)";
    const std::string node1 = "node1 (filter)";
    const std::string node2 = "node2 (map)";
    const std::string root1 = "root1 (sink)";
    const std::string root2 = "root2 (sink)";
};

class TestNode : public Operator
{
public:
    explicit TestNode(std::string textMember) : Operator(getNextOperatorId()), text(std::move(textMember)) { }
    std::string text;
    std::ostream& toDebugString(std::ostream& os) const override { return os << text; }
    /// Just implementing these so that TestNode is not virtual, we don't need any of them.
    std::shared_ptr<Operator> copy() override { return nullptr; }
    std::shared_ptr<Schema> getOutputSchema() const override { return nullptr; }
    void setOutputSchema(std::shared_ptr<Schema> outputSchema) override { (void)outputSchema; }
    std::vector<OriginId> getOutputOriginIds() const override { return {}; }
};

/// simple source filter map sink
TEST_F(LogicalQueryDumpHelperTest, printQuerySourceFilterMapSink)
{
    std::stringstream ss;
    auto helper = LogicalQueryDumpHelper(ss);

    auto leafPtr = std::make_shared<TestNode>(leaf1);
    const std::shared_ptr<Operator> leafPtrBase = std::static_pointer_cast<Operator>(leafPtr);
    auto node2Ptr = std::make_shared<TestNode>(node2);
    const std::shared_ptr<Operator> node2PtrBase = std::static_pointer_cast<Operator>(node2Ptr);
    auto node1Ptr = std::make_shared<TestNode>(node1);
    const std::shared_ptr<Operator> node1PtrBase = std::static_pointer_cast<Operator>(node1Ptr);
    auto rootPtr = std::make_shared<TestNode>(root1);
    auto rootPtrBase = std::static_pointer_cast<Operator>(rootPtr);
    rootPtr->addChild(node1PtrBase);
    node1Ptr->addChild(node2PtrBase);
    node2Ptr->addChild(leafPtrBase);

    auto rootOperators = std::vector{rootPtrBase};
    helper.dump(rootOperators);
    NES_DEBUG("Queryplan actual: {}", ss.str());

    /// We rely on the `FRIEND_TEST` macro from gtest to access `processedDag`.
    EXPECT_EQ(helper.processedDag.size(), 4);
    EXPECT_EQ(helper.processedDag[0].nodes.size(), 1);
    EXPECT_EQ(helper.processedDag[0].nodes[0]->text, root1);
    EXPECT_EQ(helper.processedDag[1].nodes.size(), 1);
    EXPECT_EQ(helper.processedDag[1].nodes[0]->text, node1);
    EXPECT_EQ(helper.processedDag[2].nodes.size(), 1);
    EXPECT_EQ(helper.processedDag[2].nodes[0]->text, node2);
    EXPECT_EQ(helper.processedDag[3].nodes.size(), 1);
    EXPECT_EQ(helper.processedDag[3].nodes[0]->text, leaf1);
}

/// multiple parents: map and filter each into its own sink
/// sink1-----\
///            +---filter---map---source
/// sink2-----/
TEST_F(LogicalQueryDumpHelperTest, printQueryMapFilterTwoSinks)
{
    std::stringstream ss;
    auto helper = LogicalQueryDumpHelper(ss);

    auto leafPtr = std::make_shared<TestNode>(leaf1);
    const std::shared_ptr<Operator> leafPtrBase = std::static_pointer_cast<Operator>(leafPtr);
    auto node2Ptr = std::make_shared<TestNode>(node2);
    const std::shared_ptr<Operator> node2PtrBase = std::static_pointer_cast<Operator>(node2Ptr);
    auto node1Ptr = std::make_shared<TestNode>(node1);
    const std::shared_ptr<Operator> node1PtrBase = std::static_pointer_cast<Operator>(node1Ptr);
    auto root1Ptr = std::make_shared<TestNode>(root1);
    auto root1PtrBase = std::static_pointer_cast<Operator>(root1Ptr);
    auto root2Ptr = std::make_shared<TestNode>(root2);
    auto root2PtrBase = std::static_pointer_cast<Operator>(root2Ptr);
    root1Ptr->addChild(node1PtrBase);
    root2Ptr->addChild(node1PtrBase);
    node1Ptr->addChild(node2PtrBase);
    node2Ptr->addChild(leafPtrBase);

    auto rootOperators = std::vector{root1PtrBase, root2PtrBase};
    helper.dump(rootOperators);
    NES_DEBUG("Queryplan actual: {}", ss.str());

    /// We rely on the `FRIEND_TEST` macro from gtest to access `processedDag`.
    EXPECT_EQ(helper.processedDag.size(), 4);
    EXPECT_EQ(helper.processedDag[0].nodes.size(), 2);
    EXPECT_EQ(helper.processedDag[0].nodes[0]->text, root1);
    EXPECT_EQ(helper.processedDag[0].nodes[1]->text, root2);
    EXPECT_EQ(helper.processedDag[1].nodes.size(), 1);
    EXPECT_EQ(helper.processedDag[1].nodes[0]->text, node1);
    EXPECT_EQ(helper.processedDag[2].nodes.size(), 1);
    EXPECT_EQ(helper.processedDag[2].nodes[0]->text, node2);
    EXPECT_EQ(helper.processedDag[3].nodes.size(), 1);
    EXPECT_EQ(helper.processedDag[3].nodes[0]->text, leaf1);
}
/// TODO #685 Add a test that forces us to add dummy nodes for nodes that have to be moved to another layer while being drawn.
/// TODO #685 Add a test that forces branches to cross.
}
