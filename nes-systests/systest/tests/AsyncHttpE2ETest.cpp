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
#include <filesystem>
#include <fstream>
#include <iterator>
#include <sstream>
#include <string>
#include <string_view>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <Config/Config.hpp>
#include <Http/DummyHttpServer.hpp>
#include <Identifiers/Identifiers.hpp>
#include <BaseUnitTest.hpp>
#include <SystestExecutor.hpp>
#include <WorkerConfig.hpp>

namespace NES::Systest
{

namespace
{

constexpr std::string_view PortPlaceholder = "__PORT__";
constexpr size_t DefaultWorkerCapacity = 1000;

std::string readFile(const std::filesystem::path& path)
{
    std::ifstream stream{path};
    return std::string{std::istreambuf_iterator<char>{stream}, std::istreambuf_iterator<char>{}};
}

std::string replaceAllOccurrences(std::string text, const std::string_view needle, const std::string_view replacement)
{
    for (auto position = text.find(needle); position != std::string::npos; position = text.find(needle, position + replacement.size()))
    {
        text.replace(position, needle.size(), replacement);
    }
    return text;
}

}

/// End-to-end proof that the asynchronous operator framework works against a real service, with no
/// language model and no other external service involved: a dummy HTTP server on the loopback
/// interface, an `HTTP_CALL` query, and the engine in between. This is the test that has to pass
/// for the framework to be reviewable on its own.
///
/// The test file cannot be a plain `.test`: the server binds an ephemeral port so that several
/// tests can run at once, and the systest configuration has no text substitution -- only worker
/// configuration keys. So the port is substituted here and the result written next to the binary,
/// which also keeps ctest from discovering a copy with a stale port in it.
class AsyncHttpE2ETest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("AsyncHttpE2ETest.log", LogLevel::LOG_DEBUG); }

    static constexpr std::string_view Extension = ".dummy";

    DummyHttpServer server;

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        server.start();
    }

    void TearDown() override
    {
        server.stop();
        BaseUnitTest::TearDown();
    }

    /// Writes the test file with the server's real port and returns where it landed. Deliberately
    /// not the working directory: `WorkingDirectoryGuard` empties that one when the run starts, so a
    /// test file put there would be gone before it could be read.
    [[nodiscard]] std::filesystem::path materializeTestFile(const std::filesystem::path& inputDirectory) const
    {
        const auto templatePath = std::filesystem::path{SYSTEST_DATA_DIR} / "AsyncHttpCall.dummy";
        const auto contents = readFile(templatePath);
        EXPECT_FALSE(contents.empty()) << "could not read " << templatePath;

        std::filesystem::create_directories(inputDirectory);
        const auto testFile = inputDirectory / fmt::format("AsyncHttpCall{}", Extension);
        std::ofstream{testFile} << replaceAllOccurrences(contents, PortPlaceholder, std::to_string(server.port()));
        return testFile;
    }
};

TEST_F(AsyncHttpE2ETest, HttpCallRunsAsynchronouslyAgainstARealServer)
{
    const auto base = std::filesystem::path{PATH_TO_BINARY_DIR} / "nes-systests" / "systest";
    const auto workingDirectory = base / "AsyncHttpCall";
    const auto testFile = materializeTestFile(base / "AsyncHttpCall-input");

    SystestConfiguration config{};
    config.testDiscoverRoot = SYSTEST_DATA_DIR;
    config.directlySpecifiedTestFiles.setValue(testFile.string());
    config.testFileExtension.setValue(std::string{Extension});
    config.workingDir.setValue(workingDirectory.string());
    config.clusterConfig = SystestClusterConfiguration{
        .workers = {WorkerConfig{
            .host = Host("localhost:8080"),
            .dataAddress = "localhost:9090",
            .maxOperators = Capacity(CapacityKind::Limited{DefaultWorkerCapacity}),
            .downstream = {},
            .config = {}}},
        .allowSourcePlacement = {Host("localhost:8080")},
        .allowSinkPlacement = {Host("localhost:8080")}};

    SystestExecutor executor(config);
    const auto result = executor.executeSystests();
    ASSERT_TRUE(result.returnType == SystestExecutorResult::ReturnType::SUCCESS) << result.outputMessage;

    /// Four records, then eight, then eight: the operator really called out rather than inventing
    /// the answers. Retries would add to this, and none of these routes fails. The EXPLAIN query
    /// adds nothing, because explaining a plan does not run it.
    EXPECT_EQ(server.requestCount(), 20);
}

}
