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

#include <chrono>
#include <memory>
#include <ranges>
#include <string>
#include <argparse/argparse.hpp>

#include <Identifiers/Identifiers.hpp>
#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <YAML/YamlBinder.hpp>
#include <YAML/YamlLoader.hpp>
#include <ErrorHandling.hpp>
#include <NESThread.hpp>
#include <QueryConfig.hpp>
#include <QueryPlanning.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

extern void enable_memcom();

int main(const int argc, char** argv)
{
    /// Initialize logging
    NES::Logger::setupLogging("nebuli.log", NES::LogLevel::LOG_INFO, true);
    NES::Thread::initializeThread(NES::WorkerId("nebuli"), "nebuli-main");

    argparse::ArgumentParser parser("nebuli-embedded");
    parser.add_argument("path").help("Path to the query configuration file");

    parser.parse_args(argc, argv);

    enable_memcom();

    const auto path = parser.get<std::string>("path");
    auto queryConfig = YAML::LoadFile(path).as<NES::CLI::QueryConfig>();
    auto parsedPlan = NES::AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(queryConfig.query);
    auto [plan, ctx] = NES::CLI::YamlBinder::from(queryConfig).bind(std::move(parsedPlan));
    auto distributedPlan = NES::QueryPlanner::with(ctx).plan(std::move(plan));

    NES::SingleNodeWorkerConfiguration configuration;
    configuration.workerConfiguration.defaultQueryExecution.executionMode = NES::ExecutionMode::INTERPRETER;
    configuration.workerConfiguration.queryEngine.numberOfWorkerThreads = 1;

    NES::QueryManager queryManager(
        std::make_unique<NES::EmbeddedWorkerQuerySubmissionBackend>(ctx.workerCatalog->getAllWorkers(), configuration));

    auto query = queryManager.registerQuery(std::move(distributedPlan));
    if (!query.has_value())
    {
        NES_ERROR("Failed to register query: {}", query.error());
        return 1;
    }

    if (auto startResponse = queryManager.start(*query); !startResponse.has_value())
    {
        NES_ERROR("Failed to start query: {}", startResponse.error());
        return 1;
    }

    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        auto status = queryManager.status(*query);
        if (!status.has_value())
        {
            NES_ERROR("Failed to receive status for query: {}", fmt::join(status.error(), ","));
            return 1;
        }
        auto queryState = status->getGlobalQueryState();
        if (queryState == NES::QueryState::Failed)
        {
            NES_ERROR("Query failed:\n\t{}", fmt::join(status->getExceptions(), "\n\t"));
            return 1;
        }
        if (queryState == NES::QueryState::Stopped)
        {
            NES_INFO("Query stopped");
            return 0;
        }
    }
}
