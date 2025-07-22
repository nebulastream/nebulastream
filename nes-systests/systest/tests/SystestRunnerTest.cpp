// /*
//     Licensed under the Apache License, Version 2.0 (the "License");
//     you may not use this file except in compliance with the License.
//     You may obtain a copy of the License at
//
//         https://www.apache.org/licenses/LICENSE-2.0
//
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.
// */
//
// #include <cstddef>
// #include <cstdint>
// #include <expected>
// #include <memory>
// #include <optional>
// #include <string>
// #include <thread>
// #include <unordered_map>
// #include <utility>
// #include <variant>
// #include <vector>
//
// #include <Identifiers/Identifiers.hpp>
// #include <Listeners/QueryLog.hpp>
// #include <Operators/Sinks/SinkLogicalOperator.hpp>
// #include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
// #include <Plans/LogicalPlan.hpp>
// #include <Runtime/Execution/QueryStatus.hpp>
// #include <Util/Logger/LogLevel.hpp>
// #include <Util/Logger/Logger.hpp>
// #include <Util/Logger/impl/NesLogger.hpp>
// #include <gmock/gmock.h>
// #include <gtest/gtest.h>
//
// #include <Identifiers/NESStrongType.hpp>
// #include <QueryManager/QueryManager.hpp>
// #include <Sources/SourceCatalog.hpp>
// #include <Sources/SourceDescriptor.hpp>
// #include <BaseUnitTest.hpp>
// #include <ErrorHandling.hpp>
// #include <SystestParser.hpp>
// #include <SystestRunner.hpp>
// #include <SystestState.hpp>
//
// namespace
// {
//
// /// NOLINTBEGIN(bugprone-unchecked-optional-access)
//
// NES::LocalQueryStatus makeSummary(const NES::LocalQueryId id, const NES::QueryState state, const std::shared_ptr<NES::Exception>& err)
// {
//     NES::LocalQueryStatus localStatus;
//     localStatus.queryId = id;
//     localStatus.state = state;
//     if (state == NES::QueryState::Failed && err)
//     {
//         localStatus.metrics.error = *err;
//     }
//     return localStatus;
// }
//
// NES::Systest::PlannedQuery makeQuery(
//     const std::expected<NES::Systest::PlanInfo, NES::Exception> planInfoOrException,
//     std::variant<std::vector<std::string>, NES::Systest::ExpectedError> expected)
// {
//     auto ctx = NES::Systest::SystestQueryContext{
//         .testName = "test_query",
//         .testFilePath = SYSTEST_DATA_DIR "filter.dummy",
//         .workingDir = NES::SystestConfiguration{}.workingDir.getValue(),
//         .queryIdInFile = NES::INVALID<NES::Systest::SystestQueryId>,
//         .queryDefinition = "SELECT * FROM test",
//         .expectedResultsOrError = std::move(expected),
//         .additionalSourceThreads = std::make_shared<std::vector<std::jthread>>(),
//     };
//
//     return NES::Systest::PlannedQuery{
//         .ctx = std::move(ctx),
//         .planInfoOrException = planInfoOrException,
//     };
// }
// }
//
// namespace NES::Systest
// {
//
// class SystestRunnerTest : public Testing::BaseUnitTest
// {
// public:
//     static void SetUpTestSuite()
//     {
//         Logger::setupLogging("SystestRunnerTest.log", LogLevel::LOG_DEBUG);
//         NES_DEBUG("Setup SystestRunnerTest test class.");
//     }
//
//     static void TearDownTestSuite() { NES_DEBUG("Tear down SystestRunnerTest test class."); }
//
//     Sinks::SinkDescriptor dummySinkDescriptor
//         = SinkCatalog{}.addSinkDescriptor("dummySink", Schema{}, "Print", "localhost", {{"inputFormat", "CSV"}}).value();
// };
//
// TEST_F(SystestRunnerTest, ExpectedErrorDuringParsing)
// {
//     const testing::InSequence seq;
//
//     constexpr ErrorCode expectedCode = ErrorCode::InvalidQuerySyntax;
//     const auto parseError = std::unexpected(Exception{"parse error", static_cast<uint64_t>(expectedCode)});
//
//     const auto result = SystestRunner::from({makeQuery(parseError, ExpectedError{.code = expectedCode, .message = std::nullopt})}, SystestRunner::LocalExecution{}, 1).run();
//     EXPECT_TRUE(result.empty()) << "query should pass because error was expected";
// }
//
// TEST_F(SystestRunnerTest, RuntimeFailureWithUnexpectedCode)
// {
//     const testing::InSequence seq;
//     constexpr LocalQueryId id{7};
//     /// Runtime fails with unexpected error code 10000
//     const auto runtimeErr = std::make_shared<Exception>(Exception{"runtime boom", 10000});
//
//     auto mockManager = std::make_unique<MockQueryManager>();
//     EXPECT_CALL(*mockManager, registerQuery(::testing::_)).WillOnce(testing::Return(std::expected<LocalQueryId, Exception>{id}));
//     EXPECT_CALL(*mockManager, start(id));
//     EXPECT_CALL(*mockManager, status(id))
//         .WillOnce(testing::Return(makeSummary(id, DistributedQueryStatus::Failed, runtimeErr)))
//         .WillRepeatedly(testing::Return(QuerySummary{}));
//
//     QuerySubmitter submitter{std::move(mockManager)};
//     SourceCatalog sourceCatalog;
//     auto testLogicalSource = sourceCatalog.addLogicalSource("testSource", Schema{});
//     auto testPhysicalSource
//         = sourceCatalog.addPhysicalSource(testLogicalSource.value(), "File", {{"filePath", "/dev/null"}}, ParserConfig{});
//     auto sourceOperator = SourceDescriptorLogicalOperator{testPhysicalSource.value()}.withOutputOriginIds({OriginId{1}});
//     const LogicalPlan plan{SinkLogicalOperator{dummySinkDescriptor}.withChildren({sourceOperator})};
//
//     const auto result = runQueries(
//         {makeQuery(SystestQuery::PlanInfo{.queryPlan = plan, .sourcesToFilePathsAndCounts = {}, .sinkOutputSchema = Schema{}}, {})},
//         1,
//         submitter);
//
//     ASSERT_EQ(result.size(), 1);
//     EXPECT_FALSE(result.front().passed);
//     EXPECT_EQ(result.front().exception->code(), 10000);
// }
//
// TEST_F(SystestRunnerTest, MissingExpectedRuntimeError)
// {
//     const testing::InSequence seq;
//     constexpr LocalQueryId id{11};
//
//     auto mockManager = std::make_unique<MockQueryManager>();
//     EXPECT_CALL(*mockManager, registerQuery(::testing::_)).WillOnce(testing::Return(std::expected<LocalQueryId, Exception>{id}));
//     EXPECT_CALL(*mockManager, start(id));
//     EXPECT_CALL(*mockManager, status(id))
//         .WillOnce(testing::Return(makeSummary(id, QueryState::Stopped, nullptr)))
//         .WillRepeatedly(testing::Return(QuerySummary{}));
//
//     QuerySubmitter submitter{std::move(mockManager)};
//     SourceCatalog sourceCatalog;
//     auto testLogicalSource = sourceCatalog.addLogicalSource("testSource", Schema{});
//     auto testPhysicalSource
//         = sourceCatalog.addPhysicalSource(testLogicalSource.value(), "File", {{"filePath", "/dev/null"}}, ParserConfig{});
//     auto sourceOperator = SourceDescriptorLogicalOperator{testPhysicalSource.value()}.withOutputOriginIds({OriginId{1}});
//     const LogicalPlan plan{SinkLogicalOperator{dummySinkDescriptor}.withChildren({sourceOperator})};
//
//     const auto result = runQueries(
//         {makeQuery(
//             SystestQuery::PlanInfo{.queryPlan = plan, .sourcesToFilePathsAndCounts = {}, .sinkOutputSchema = Schema{}},
//             ExpectedError{.code = ErrorCode::InvalidQuerySyntax, .message = std::nullopt})},
//         1,
//         submitter);
//
//     ASSERT_EQ(result.size(), 1);
//     EXPECT_FALSE(result.front().passed);
// }
//
// /// NOLINTEND(bugprone-unchecked-optional-access)
// }
