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

#include <BaseUnitTest.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <CoordinatorRPCService_mock.grpc.pb.h>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <fstream>
#include <gmock/gmock-spec-builders.h>
#include <memory>
#include <regex>

namespace NES {

class CoordinatorRPCClientTest : public Testing::BaseUnitTest {
  public:
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        // IMPORTANT: We reset the logger for every testcase, because we are expecting a specific error message!
        setupLogging();
    }

    void TearDown() override {
        cleanUplogging();
        Testing::BaseUnitTest::TearDown();
    }

    static void SetUpTestCase() {}
    constexpr static auto VALID_LOGICAL_SOURCE_NAME = "ValidSource";
    constexpr static auto INVALID_LOGICAL_SOURCE_NAME = "InvalidSource";

  protected:
    void setupLogging() {
        if (std::tmpnam(tmpFilePath)) {
            NES::Logger::setupLogging(tmpFilePath, NES::LogLevel::LOG_DEBUG);
        } else {
            std::cerr << "Could not create temporary file for logging";
        }
    }

    void cleanUplogging() const { unlink(tmpFilePath); }

    static std::tuple<std::unique_ptr<CoordinatorRPCClient>, MockCoordinatorRPCServiceStub&> defaultUUT() {
        using namespace std::chrono_literals;
        auto stub = std::make_unique<MockCoordinatorRPCServiceStub>();
        auto& stubRef = *stub;
        return {std::make_unique<CoordinatorRPCClient>(std::move(stub), "123.123.123.123", 2, 15ms), stubRef};
    }

    std::function<bool(std::string_view)> getLineFilter(LogLevel logLevel) const {
        // Assumes the standard SPDLOG_PATTERN
        // Assumes that the log level is represented by [X] where X is the first letter of the LogLevel with the `LOG_` prefix
        // LOG_ERROR - E, LOG_WARNING - W, LOG_INFO - I, ...
        std::regex timestampRegex("^\\[\\d{2}:\\d{2}:\\d{2}\\.\\d{6}\\] \\["
                                  + std::string(magic_enum::enum_name(logLevel)).substr(4, 1) + "\\] ");
        return [timestampRegex = std::move(timestampRegex)](const std::string_view line) {
            return std::regex_search(line.begin(), line.end(), timestampRegex);
        };
    }

    std::function<bool(std::string_view)> getNoOpLineFilter() const {
        return [](const std::string_view) {
            return true;
        };
    }

    void assertErrorMessageInLogOutput(std::string_view errorMessage,
                                       size_t numberOfOccurences = 1,
                                       std::optional<LogLevel> logLevel = std::nullopt) const {

        // Flush Logs, otherwise logs may not have been written to disk
        NES::Logger::getInstance()->forceFlush();
        std::ifstream log(tmpFilePath);
        EXPECT_TRUE(log) << "Could not open the " << tmpFilePath << "log file";

        if (logLevel) {
            EXPECT_GE(NES_COMPILE_TIME_LOG_LEVEL, getLogLevel(*logLevel))
                << "NES_COMPILE_TIME_LOG_LEVEL prevents log output assertions on level: " << magic_enum::enum_name(*logLevel);
            EXPECT_NE(LogLevel::LOG_NONE, *logLevel) << "Testing for logs with LogLevel::LOG_NONE is pointless";
        }
        auto lineFilterFunction = logLevel ? getLineFilter(*logLevel) : getNoOpLineFilter();

        size_t occurences = 0;
        std::string line;
        for (unsigned int curLine = 0; getline(log, line); curLine++) {
            if (!lineFilterFunction(line)) {
                continue;
            }

            if (line.find(errorMessage) != std::string::npos) {
                occurences++;
            }
        }

        EXPECT_EQ(occurences, numberOfOccurences)
            << fmt::format("Could not find the Error Message \"{}\" in the log {} times", errorMessage, numberOfOccurences);
    }

  private:
    char tmpFilePath[L_tmpnam] = {};
};

TEST_F(CoordinatorRPCClientTest, TestEmptyPhysicalSourceRegistrationRequest) {
    using namespace ::testing;
    auto [uut, mock] = defaultUUT();
    RegisterPhysicalSourcesReply reply;
    reply.set_success(true);

    RegisterPhysicalSourcesRequest expected;
    EXPECT_CALL(mock, RegisterPhysicalSource(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SaveArg<1>(&expected), SetArgPointee<2>(reply), Return(Status::OK)));

    EXPECT_TRUE(uut->registerPhysicalSources({}));
    EXPECT_EQ(expected.workerid(), INVALID_WORKER_NODE_ID.getRawValue());
    EXPECT_THAT(expected.physicalsourcetypes(), IsEmpty());
}

TEST_F(CoordinatorRPCClientTest, TestValidPhysicalSourceRegistrationRequest) {
    using namespace ::testing;
    auto [uut, mock] = defaultUUT();
    RegisterPhysicalSourcesReply reply;
    reply.set_success(true);
    auto result = reply.add_results();
    result->set_success(true);
    result->set_physicalsourcename(fmt::format("{}_{}", VALID_LOGICAL_SOURCE_NAME, 1));
    result->set_reason("");

    RegisterPhysicalSourcesRequest expected;
    EXPECT_CALL(mock, RegisterPhysicalSource(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SaveArg<1>(&expected), SetArgPointee<2>(reply), Return(Status::OK)));

    auto valid = DefaultSourceType::create(VALID_LOGICAL_SOURCE_NAME, fmt::format("{}_{}", VALID_LOGICAL_SOURCE_NAME, 1));

    EXPECT_TRUE(uut->registerPhysicalSources({valid}));
    EXPECT_EQ(expected.workerid(), INVALID_WORKER_NODE_ID.getRawValue());
    EXPECT_THAT(expected.physicalsourcetypes(), SizeIs(1));
    EXPECT_EQ(expected.physicalsourcetypes(0).logicalsourcename(), VALID_LOGICAL_SOURCE_NAME);
    EXPECT_EQ(expected.physicalsourcetypes(0).physicalsourcename(), fmt::format("{}_{}", VALID_LOGICAL_SOURCE_NAME, 1));
    EXPECT_EQ(expected.physicalsourcetypes(0).sourcetype(), magic_enum::enum_name(SourceType::DEFAULT_SOURCE));
}

TEST_F(CoordinatorRPCClientTest, TestValidAndInvalidPhysicalSourceRegistrationRequest) {
    using namespace ::testing;
    auto [uut, mock] = defaultUUT();
    RegisterPhysicalSourcesReply reply;
    reply.set_success(false);
    {
        auto result = reply.add_results();
        result->set_success(true);
        result->set_physicalsourcename(fmt::format("{}_{}", VALID_LOGICAL_SOURCE_NAME, 1));
        result->set_reason("");
    }
    {
        auto result = reply.add_results();
        result->set_success(false);
        result->set_physicalsourcename(fmt::format("{}_{}", INVALID_LOGICAL_SOURCE_NAME, 1));
        result->set_reason("IF_THIS_APPEARS_IN_THE_ERROR_MESSAGE_THE_TEST_WAS_SUCCESSFUL");
    }

    RegisterPhysicalSourcesRequest expected;
    EXPECT_CALL(mock, RegisterPhysicalSource(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SaveArg<1>(&expected), SetArgPointee<2>(reply), Return(Status::OK)));

    auto valid = DefaultSourceType::create(VALID_LOGICAL_SOURCE_NAME, fmt::format("{}_{}", VALID_LOGICAL_SOURCE_NAME, 1));
    auto invalid = DefaultSourceType::create(INVALID_LOGICAL_SOURCE_NAME, fmt::format("{}_{}", INVALID_LOGICAL_SOURCE_NAME, 1));

    EXPECT_FALSE(uut->registerPhysicalSources({valid, invalid}));
    EXPECT_EQ(expected.workerid(), INVALID_WORKER_NODE_ID.getRawValue());
    EXPECT_THAT(expected.physicalsourcetypes(), SizeIs(2));
    EXPECT_EQ(expected.physicalsourcetypes(0).logicalsourcename(), VALID_LOGICAL_SOURCE_NAME);
    EXPECT_EQ(expected.physicalsourcetypes(0).physicalsourcename(), fmt::format("{}_{}", VALID_LOGICAL_SOURCE_NAME, 1));
    EXPECT_EQ(expected.physicalsourcetypes(0).sourcetype(), magic_enum::enum_name(SourceType::DEFAULT_SOURCE));

    EXPECT_EQ(expected.physicalsourcetypes(1).logicalsourcename(), INVALID_LOGICAL_SOURCE_NAME);
    EXPECT_EQ(expected.physicalsourcetypes(1).physicalsourcename(), fmt::format("{}_{}", INVALID_LOGICAL_SOURCE_NAME, 1));
    EXPECT_EQ(expected.physicalsourcetypes(1).sourcetype(), magic_enum::enum_name(SourceType::DEFAULT_SOURCE));

    assertErrorMessageInLogOutput("IF_THIS_APPEARS_IN_THE_ERROR_MESSAGE_THE_TEST_WAS_SUCCESSFUL", 1, LogLevel::LOG_ERROR);
}

TEST_F(CoordinatorRPCClientTest, TestGenericErrorPhysicalSourceRegistrationRequest) {
    using namespace ::testing;
    auto [uut, mock] = defaultUUT();

    EXPECT_CALL(mock, RegisterPhysicalSource(_, _, _))
        .Times(3)// Initial + 2 Retries
        .WillRepeatedly(Return(Status(grpc::UNAVAILABLE, "THIS_SHOULD_APPEAR_IN_THE_LOG")));

    auto valid = DefaultSourceType::create(VALID_LOGICAL_SOURCE_NAME, fmt::format("{}_{}", VALID_LOGICAL_SOURCE_NAME, 1));

    EXPECT_FALSE(uut->registerPhysicalSources({valid}));

    assertErrorMessageInLogOutput("THIS_SHOULD_APPEAR_IN_THE_LOG", 3, LogLevel::LOG_WARNING);
}

}// namespace NES
