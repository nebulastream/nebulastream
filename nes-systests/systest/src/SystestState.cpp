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

#include <SystestState.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <expected> /// NOLINT(misc-include-cleaner)
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <regex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>


#include <Identifiers/Identifiers.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h> ///NOLINT: required by fmt

#include <Runner/SystestRunner.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>

namespace NES::Systest
{

std::filesystem::path SystestQuery::resultFile(
    const std::filesystem::path& workingDir, std::string_view testName, const SystestQueryId queryIdInTestFile, const size_t sinkIndex)
{
    auto resultPath = workingDir / "results" / std::filesystem::path(fmt::format("{}_{}.csv", testName, queryIdInTestFile));
    const auto resultDir = resultPath.parent_path();
    if (not is_directory(resultDir))
    {
        create_directories(resultDir);
        std::cout << "Created working directory: file://" << resultDir.string() << "\n";
    }

    /// The first sink keeps the plain name, so result files of single-sink queries stay recognizable.
    if (sinkIndex == 0)
    {
        return resultPath;
    }
    return resultDir / std::filesystem::path(fmt::format("{}_{}_sink{}.csv", testName, queryIdInTestFile, sinkIndex));
}

std::filesystem::path SystestQuery::sourceFile(const std::filesystem::path& workingDir, std::string_view testName, const uint64_t sourceId)
{
    auto sourcePath = workingDir / "sources" / std::filesystem::path(fmt::format("{}_{}.csv", testName, sourceId));
    const auto sourceDir = sourcePath.parent_path();
    if (not is_directory(sourceDir))
    {
        create_directories(sourceDir);
        std::cout << "Created working directory: file://" << sourceDir.string() << "\n";
    }

    return sourcePath;
}

std::filesystem::path SystestQuery::resultFile(const size_t sinkIndex) const
{
    return resultFile(workingDir, testName, queryIdInFile, sinkIndex);
}

std::filesystem::path SystestQuery::resultFileForDifferentialQuery() const
{
    return resultFile(workingDir, testName + "differential", queryIdInFile);
}

std::vector<TestGroup> readGroups(const TestFile& testfile)
{
    std::vector<TestGroup> groups;
    if (std::ifstream ifstream(testfile.file); ifstream.is_open())
    {
        std::string line;
        while (std::getline(ifstream, line))
        {
            if (line.starts_with("# groups:"))
            {
                auto content = std::string_view(line).substr(9);
                auto open = content.find('[');
                auto close = content.find(']');
                auto inner = content.substr(open + 1, close - open - 1);
                for (auto part : inner | std::views::split(',') | std::views::transform([](auto r) { return std::string_view(r); })
                         | std::views::transform([](auto sv) { return sv | std::views::filter([](char c) { return !std::isspace(c); }); }))
                {
                    groups.emplace_back(std::ranges::to<std::string>(part));
                }
                break;
            }
        }
        ifstream.close();
    }
    return groups;
}

TestFile::TestFile(
    const std::filesystem::path& file, std::shared_ptr<SourceCatalog> sourceCatalog, std::shared_ptr<SinkCatalog> sinkCatalog)
    : TestFile(file, file.stem().string(), std::move(sourceCatalog), std::move(sinkCatalog))
{
}

TestFile::TestFile(
    const std::filesystem::path& file,
    TestName testName,
    std::shared_ptr<SourceCatalog> sourceCatalog,
    std::shared_ptr<SinkCatalog> sinkCatalog)
    : file(weakly_canonical(file))
    , testName(std::move(testName))
    , groups(readGroups(*this))
    , sourceCatalog(std::move(sourceCatalog))
    , sinkCatalog(std::move(sinkCatalog)) { };

TestFile::TestFile(
    const std::filesystem::path& file,
    std::unordered_set<SystestQueryId> onlyEnableQueriesWithTestQueryNumber,
    std::shared_ptr<SourceCatalog> sourceCatalog,
    std::shared_ptr<SinkCatalog> sinkCatalog)
    : TestFile(
          file, std::move(onlyEnableQueriesWithTestQueryNumber), file.stem().string(), std::move(sourceCatalog), std::move(sinkCatalog))
{
}

TestFile::TestFile(
    const std::filesystem::path& file,
    std::unordered_set<SystestQueryId> onlyEnableQueriesWithTestQueryNumber,
    TestName testName,
    std::shared_ptr<SourceCatalog> sourceCatalog,
    std::shared_ptr<SinkCatalog> sinkCatalog)
    : file(weakly_canonical(file))
    , testName(std::move(testName))
    , onlyEnableQueriesWithTestQueryNumber(std::move(onlyEnableQueriesWithTestQueryNumber))
    , groups(readGroups(*this))
    , sourceCatalog(std::move(sourceCatalog))
    , sinkCatalog(std::move(sinkCatalog)) { };

std::chrono::duration<double> RunningQuery::getElapsedTime() const
{
    INVARIANT(queryId != DistributedQueryId(DistributedQueryId::INVALID), "QueryId should not be invalid");
    INVARIANT(queryStatus.has_value(), "Query should have a status, otherwise it failed during registration already.");
    const auto metrics = queryStatus.value().coalesceQueryMetrics();
    const auto stop = metrics.stop;
    const auto running = metrics.running;
    INVARIANT(stop.has_value() && running.has_value(), "Query {} has no timestamps attached", queryId);
    return std::chrono::duration_cast<std::chrono::duration<double>>(stop.value() - running.value());
}

std::string RunningQuery::getThroughput() const
{
    INVARIANT(queryId != DistributedQueryId(DistributedQueryId::INVALID), "QueryId should not be invalid");
    INVARIANT(queryStatus.has_value(), "Query should have a status, otherwise it failed during registration already.");
    const auto metrics = queryStatus.value().coalesceQueryMetrics();

    const auto stop = metrics.stop;
    const auto running = metrics.running;
    INVARIANT(stop.has_value() && running.has_value(), "Query {} has no timestamps timestamps attached", queryId);
    if (not bytesProcessed.has_value() or not tuplesProcessed.has_value())
    {
        return "";
    }

    double bytesPerSecond = NAN;
    double tuplesPerSecond = NAN;
    if (bytesProcessed.value() > 0 and tuplesProcessed.value() > 0)
    {
        const std::chrono::duration<double> duration = stop.value() - running.value();
        bytesPerSecond = static_cast<double>(bytesProcessed.value()) / duration.count();
        tuplesPerSecond = static_cast<double>(tuplesProcessed.value()) / duration.count();
    }

    auto formatUnits = [](double throughput)
    {
        const std::array<std::string, 5> units = {"", "k", "M", "G", "T"};
        uint64_t unitIndex = 0;
        constexpr auto nextUnit = 1000;
        while (throughput >= nextUnit && unitIndex < units.size() - 1)
        {
            throughput /= nextUnit;
            unitIndex++;
        }
        return fmt::format("{:.3f} {}", throughput, units[unitIndex]);
    };
    return fmt::format("{}B/s / {}Tup/s", formatUnits(bytesPerSecond), formatUnits(tuplesPerSecond));
}

std::string TestFile::getLogFilePath() const
{
    if (const char* hostNebulaStreamRoot = std::getenv("HOST_NEBULASTREAM_ROOT"))
    {
        auto commonFolder = std::filesystem::path(hostNebulaStreamRoot).filename();

        auto filePathIter = file.begin();
        if (const auto it = std::ranges::find(file, commonFolder); it != file.end())
        {
            filePathIter = std::next(it);
        }

        std::filesystem::path resultPath(hostNebulaStreamRoot);
        for (; filePathIter != file.end(); ++filePathIter)
        {
            resultPath /= *filePathIter;
        }

        return resultPath.string();
    }

    return std::filesystem::path(file);
}
}
