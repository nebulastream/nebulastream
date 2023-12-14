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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/Expressions/ConstantValueExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceOperator.hpp>
#include <Plans/ChangeLog/ChangeLog.hpp>
#include <Plans/ChangeLog/ChangeLogEntry.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/DumpHandler/ConsoleDumpHandler.hpp>
#include <Util/DumpHandler/DumpContext.hpp>
#include <gtest/gtest.h>

namespace NES {

class ChangeLogTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ChangeLogTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ChangeLogTest test class.");
    }

    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        dumpContext = DumpContext::create();
        dumpContext->registerDumpHandler(ConsoleDumpHandler::create(std::cout));
        pred1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "1"));
        pred2 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "1"));
        pred3 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "1"));
        sourceOp1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
        sourceOp2 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
        filterOp1 = LogicalOperatorFactory::createFilterOperator(pred1);
        filterOp2 = LogicalOperatorFactory::createFilterOperator(pred2);
        filterOp3 = LogicalOperatorFactory::createFilterOperator(pred3);
        sinkOp1 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
        sinkOp2 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    }

  protected:
    DumpContextPtr dumpContext;

    ExpressionNodePtr pred1;
    ExpressionNodePtr pred2;
    ExpressionNodePtr pred3;
    LogicalOperatorNodePtr sourceOp1;
    LogicalOperatorNodePtr sourceOp2;
    LogicalOperatorNodePtr filterOp1;
    LogicalOperatorNodePtr filterOp2;
    LogicalOperatorNodePtr filterOp3;
    LogicalOperatorNodePtr sinkOp1;
    LogicalOperatorNodePtr sinkOp2;
};

//Insert and fetch change log entry
TEST_F(ChangeLogTest, InsertAndFetchChangeLogEntry) {

    // Compute Plan
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    NES_DEBUG("{}", queryPlan->toString());

    // Initialize change log
    auto changeLog = NES::Optimizer::Experimental::ChangeLog::create();
    auto changelogEntry = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {sinkOp1});
    changeLog->addChangeLogEntry(1, std::move(changelogEntry));

    // Fetch change log entries before timestamp 1
    auto extractedChangeLogEntries = changeLog->getCompactChangeLogEntriesBeforeTimestamp(1);
    EXPECT_EQ(extractedChangeLogEntries.size(), 0);

    // Fetch change log entries before timestamp 2
    extractedChangeLogEntries = changeLog->getCompactChangeLogEntriesBeforeTimestamp(2);
    EXPECT_EQ(extractedChangeLogEntries.size(), 1);
}

//Insert and fetch change log entries
TEST_F(ChangeLogTest, InsertAndFetchMultipleChangeLogEntries) {

    // Compute Plan
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    NES_DEBUG("{}", queryPlan->toString());

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
    auto extractedChangeLogEntries = changeLog->getCompactChangeLogEntriesBeforeTimestamp(4);
    EXPECT_EQ(1, extractedChangeLogEntries.size());

    const auto actualUpstreamOperators = extractedChangeLogEntries[0].second->upstreamOperators;
    EXPECT_EQ(1, actualUpstreamOperators.size());
    EXPECT_EQ(sourceOp1, (*actualUpstreamOperators.begin()));

    const auto actualDownstreamOperators = extractedChangeLogEntries[0].second->downstreamOperators;
    EXPECT_EQ(1, actualDownstreamOperators.size());
    EXPECT_EQ(sinkOp1, (*actualDownstreamOperators.begin()));
}

//Insert and fetch change log entries
TEST_F(ChangeLogTest, UpdateChangeLogProcessingTime) {

    // Compute Plan
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp2);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    NES_DEBUG("{}", queryPlan->toString());

    // Initialize change log
    auto changeLog = NES::Optimizer::Experimental::ChangeLog::create();

    //three overlapping change log entries
    auto changelogEntry1 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {filterOp1});
    auto changelogEntry2 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {filterOp1});
    auto changelogEntry3 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {filterOp1});

    //two more overlapping change log entries
    auto changelogEntry4 = NES::Optimizer::Experimental::ChangeLogEntry::create({filterOp2}, {sinkOp1});
    auto changelogEntry5 = NES::Optimizer::Experimental::ChangeLogEntry::create({filterOp2}, {sinkOp1});

    //one more overlapping change log entries
    auto changelogEntry6 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {sinkOp1});

    changeLog->addChangeLogEntry(1, std::move(changelogEntry1));
    changeLog->addChangeLogEntry(3, std::move(changelogEntry3));
    changeLog->addChangeLogEntry(6, std::move(changelogEntry6));
    changeLog->addChangeLogEntry(5, std::move(changelogEntry5));
    changeLog->addChangeLogEntry(4, std::move(changelogEntry4));
    changeLog->addChangeLogEntry(2, std::move(changelogEntry2));

    // Fetch change log entries before timestamp 4
    auto extractedChangeLogEntries = changeLog->getCompactChangeLogEntriesBeforeTimestamp(4);
    EXPECT_EQ(1, extractedChangeLogEntries.size());

    const auto actualUpstreamOperators = extractedChangeLogEntries[0].second->upstreamOperators;
    EXPECT_EQ(1, actualUpstreamOperators.size());
    EXPECT_EQ(sourceOp1, (*actualUpstreamOperators.begin()));

    const auto actualDownstreamOperators = extractedChangeLogEntries[0].second->downstreamOperators;
    EXPECT_EQ(1, actualDownstreamOperators.size());
    EXPECT_EQ(filterOp1, (*actualDownstreamOperators.begin()));

    //Update the processing time
    changeLog->updateProcessedChangeLogTimestamp(4);

    // Fetch change log entries before timestamp 4
    extractedChangeLogEntries = changeLog->getCompactChangeLogEntriesBeforeTimestamp(4);
    EXPECT_EQ(extractedChangeLogEntries.size(), 0);

    // Fetch change log entries before timestamp 7
    extractedChangeLogEntries = changeLog->getCompactChangeLogEntriesBeforeTimestamp(7);
    //Will return one change log entry as it will merge change logs between 4 till 6
    EXPECT_EQ(1, extractedChangeLogEntries.size());

    const auto actualUpstreamOperatorsNext = extractedChangeLogEntries[0].second->upstreamOperators;
    EXPECT_EQ(1, actualUpstreamOperatorsNext.size());
    EXPECT_EQ(sourceOp1, (*actualUpstreamOperatorsNext.begin()));

    const auto actualDownstreamOperatorsNext = extractedChangeLogEntries[0].second->downstreamOperators;
    EXPECT_EQ(1, actualDownstreamOperatorsNext.size());
    EXPECT_EQ(sinkOp1, (*actualDownstreamOperatorsNext.begin()));
}

/**
 * @brief Fetch change log entry with partially overlapping change log entries
 *
 * Query:
 *
 * Sink 2 --- Filter3 ---                  --- Filter2 --- Source 2
 *                       \               /
 *                        --- Filter1 ---
 *                       |               \
 * Sink 1 ---------------                 --- Source 1
 *
 */
TEST_F(ChangeLogTest, PerformLogCompactionForPartiallyOverlappingChangeLogs) {

    // Compute Plan
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    filterOp1->addChild(filterOp2);
    filterOp2->addChild(sourceOp2);
    filterOp1->addParent(filterOp3);
    filterOp3->addParent(sinkOp2);

    // Initialize change log
    auto changeLog = NES::Optimizer::Experimental::ChangeLog::create();
    auto changelogEntry1 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1}, {filterOp1});
    auto changelogEntry2 = NES::Optimizer::Experimental::ChangeLogEntry::create({filterOp2}, {sinkOp1});
    changeLog->addChangeLogEntry(1, std::move(changelogEntry1));
    changeLog->addChangeLogEntry(2, std::move(changelogEntry2));

    // Fetch change log entries before timestamp 4
    auto extractedChangeLogEntries = changeLog->getCompactChangeLogEntriesBeforeTimestamp(3);
    //Will return one change log entry as it will merge change logs between 1 till 2
    EXPECT_EQ(1, extractedChangeLogEntries.size());

    const auto actualUpstreamOperatorsNext = extractedChangeLogEntries[0].second->upstreamOperators;
    EXPECT_EQ(2, actualUpstreamOperatorsNext.size());
    EXPECT_EQ(sourceOp1, (*actualUpstreamOperatorsNext.begin()));
    EXPECT_EQ(filterOp2, (*(++(actualUpstreamOperatorsNext.begin()))));

    const auto actualDownstreamOperatorsNext = extractedChangeLogEntries[0].second->downstreamOperators;
    EXPECT_EQ(1, actualDownstreamOperatorsNext.size());
    EXPECT_EQ(sinkOp1, (*actualDownstreamOperatorsNext.begin()));
}

/**
 * @brief Fetch change log entry with non overlapping change log entries
 *
 * Query:
 *
 * Sink 2 --- Filter3 ---                  --- Filter2--- Source 2
 *                       \               /
 *                        --- Filter1 ---
 *                       |               \
 * Sink 1 ---------------                 --- Source 1
 *
 */
TEST_F(ChangeLogTest, PerformLogCompactionForPartiallyNonOverlappingChangeLogs) {

    // Compute Plan
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    filterOp1->addChild(filterOp2);
    filterOp2->addChild(sourceOp2);
    filterOp1->addParent(filterOp3);
    filterOp3->addParent(sinkOp2);

    // Initialize change log
    auto changeLog = NES::Optimizer::Experimental::ChangeLog::create();
    auto changelogEntry1 = NES::Optimizer::Experimental::ChangeLogEntry::create({sourceOp1, sourceOp2}, {filterOp2});
    auto changelogEntry2 = NES::Optimizer::Experimental::ChangeLogEntry::create({filterOp3}, {sinkOp1});
    changeLog->addChangeLogEntry(1, std::move(changelogEntry1));
    changeLog->addChangeLogEntry(2, std::move(changelogEntry2));

    // Fetch change log entries before timestamp 4
    auto extractedChangeLogEntries = changeLog->getCompactChangeLogEntriesBeforeTimestamp(3);
    //Will return one change log entry as it will merge change logs between 1 till 2
    EXPECT_EQ(2, extractedChangeLogEntries.size());

    const auto actualUpstreamOperators = extractedChangeLogEntries[0].second->upstreamOperators;
    EXPECT_EQ(2, actualUpstreamOperators.size());
    EXPECT_EQ(sourceOp1, (*actualUpstreamOperators.begin()));
    EXPECT_EQ(sourceOp2, (*(++(actualUpstreamOperators.begin()))));

    const auto actualDownstreamOperators = extractedChangeLogEntries[0].second->downstreamOperators;
    EXPECT_EQ(1, actualDownstreamOperators.size());
    EXPECT_EQ(filterOp2, (*actualDownstreamOperators.begin()));

    const auto actualUpstreamOperatorsNext = extractedChangeLogEntries[1].second->upstreamOperators;
    EXPECT_EQ(1, actualUpstreamOperatorsNext.size());
    EXPECT_EQ(filterOp3, (*actualUpstreamOperatorsNext.begin()));

    const auto actualDownstreamOperatorsNext = extractedChangeLogEntries[1].second->downstreamOperators;
    EXPECT_EQ(1, actualDownstreamOperatorsNext.size());
    EXPECT_EQ(sinkOp1, (*actualDownstreamOperatorsNext.begin()));
}
}// namespace NES
