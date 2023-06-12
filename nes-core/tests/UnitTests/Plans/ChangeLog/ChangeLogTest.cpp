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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <NesBaseTest.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Plans/ChangeLog/ChangeLog.hpp>
#include <Plans/ChangeLog/ChangeLogEntry.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <gtest/gtest.h>

namespace NES {

class ChangeLogTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ChangeLogTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO2("Setup ChangeLogTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        dumpContext = DumpContext::create();
        dumpContext->registerDumpHandler(ConsoleDumpHandler::create(std::cout));
        pred1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "1"));
        sourceOp1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
        filterOp1 = LogicalOperatorFactory::createFilterOperator(pred1);
        sinkOp1 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    }

  protected:
    DumpContextPtr dumpContext;

    ExpressionNodePtr pred1;
    LogicalOperatorNodePtr sourceOp1;

    LogicalOperatorNodePtr filterOp1;
    LogicalOperatorNodePtr sinkOp1;
};

//Insert and fetch change log entry
TEST_F(ChangeLogTest, InsertAndFetchChangeLogEntry) {

    // Compute Plan
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    NES_DEBUG2("{}", queryPlan->toString());

    // Initialize change log
    auto changeLog = NES::Optimizer::Experimental::ChangeLog::create();
    auto changelogEntry = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {sinkOp1});
    changeLog->addChangeLogEntry(1, std::move(changelogEntry));

    // Fetch change log entries before timestamp 1
    auto extractedChangeLogEntries = changeLog->getChangeLogEntriesBefore(1);
    EXPECT_EQ(extractedChangeLogEntries.size(), 0);

    // Fetch change log entries before timestamp 2
    extractedChangeLogEntries = changeLog->getChangeLogEntriesBefore(2);
    EXPECT_EQ(extractedChangeLogEntries.size(), 1);
}

//Insert and fetch change log entries
TEST_F(ChangeLogTest, InsertAndFetchMultipleChangeLogEntries) {

    // Compute Plan
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    NES_DEBUG2("{}", queryPlan->toString());

    // Initialize change log
    auto changeLog = NES::Optimizer::Experimental::ChangeLog::create();
    auto changelogEntry1 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {sinkOp1});
    auto changelogEntry2 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {sinkOp1});
    auto changelogEntry3 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {sinkOp1});
    auto changelogEntry4 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {sinkOp1});
    auto changelogEntry5 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {sinkOp1});
    auto changelogEntry6 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {sinkOp1});
    changeLog->addChangeLogEntry(1, std::move(changelogEntry1));
    changeLog->addChangeLogEntry(3, std::move(changelogEntry3));
    changeLog->addChangeLogEntry(6, std::move(changelogEntry6));
    changeLog->addChangeLogEntry(5, std::move(changelogEntry5));
    changeLog->addChangeLogEntry(4, std::move(changelogEntry4));
    changeLog->addChangeLogEntry(2, std::move(changelogEntry2));

    // Fetch change log entries before timestamp 4
    auto extractedChangeLogEntries = changeLog->getChangeLogEntriesBefore(4);
    EXPECT_EQ(extractedChangeLogEntries.size(), 3);
    EXPECT_EQ(changelogEntry1, extractedChangeLogEntries[0]);
    EXPECT_EQ(changelogEntry2, extractedChangeLogEntries[1]);
    EXPECT_EQ(changelogEntry3, extractedChangeLogEntries[2]);
}

//Insert and fetch change log entries
TEST_F(ChangeLogTest, UpdateChangeLogProcessingTime) {

    // Compute Plan
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    NES_DEBUG2("{}", queryPlan->toString());

    // Initialize change log
    auto changeLog = NES::Optimizer::Experimental::ChangeLog::create();
    auto changelogEntry1 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {sinkOp1});
    auto changelogEntry2 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {sinkOp1});
    auto changelogEntry3 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {sinkOp1});
    auto changelogEntry4 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {sinkOp1});
    auto changelogEntry5 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {sinkOp1});
    auto changelogEntry6 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {sinkOp1});
    changeLog->addChangeLogEntry(1, std::move(changelogEntry1));
    changeLog->addChangeLogEntry(3, std::move(changelogEntry3));
    changeLog->addChangeLogEntry(6, std::move(changelogEntry6));
    changeLog->addChangeLogEntry(5, std::move(changelogEntry5));
    changeLog->addChangeLogEntry(4, std::move(changelogEntry4));
    changeLog->addChangeLogEntry(2, std::move(changelogEntry2));

    // Fetch change log entries before timestamp 4
    auto extractedChangeLogEntries = changeLog->getChangeLogEntriesBefore(4);
    EXPECT_EQ(extractedChangeLogEntries.size(), 3);
    EXPECT_EQ(changelogEntry1, extractedChangeLogEntries[0]);
    EXPECT_EQ(changelogEntry2, extractedChangeLogEntries[1]);
    EXPECT_EQ(changelogEntry3, extractedChangeLogEntries[2]);

    //Update the processing time
    changeLog->updateProcessedChangeLogTimestamp(4);

    // Fetch change log entries before timestamp 4
    extractedChangeLogEntries = changeLog->getChangeLogEntriesBefore(4);
    EXPECT_EQ(extractedChangeLogEntries.size(), 0);

    // Fetch change log entries before timestamp 7
    extractedChangeLogEntries = changeLog->getChangeLogEntriesBefore(7);
    EXPECT_EQ(extractedChangeLogEntries.size(), 3);
    EXPECT_EQ(changelogEntry4, extractedChangeLogEntries[0]);
    EXPECT_EQ(changelogEntry5, extractedChangeLogEntries[1]);
    EXPECT_EQ(changelogEntry6, extractedChangeLogEntries[2]);
}

}// namespace NES
