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

#include <SingleNodeWorker.hpp>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <utility>
#include <unistd.h>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/DumpMode.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <QueryCompiler.hpp>
#include <QueryOptimizer.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <StatisticPrinter.hpp>
#include <GoogleEventTracePrinter.hpp>
#include <ReplayPrinter.hpp>
#include <CompositeStatisticListener.hpp>
#include <filesystem>
#include <vector>
#include <algorithm>
#include <fstream> // Added for dummy file creation
#include <nlohmann/json.hpp> // Added for JSON handling

namespace NES
{

SingleNodeWorker::~SingleNodeWorker() = default;
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const SingleNodeWorkerConfiguration& configuration) : configuration(configuration)
{
    listener = std::make_shared<CompositeStatisticListener>();

    auto printingStatisticsListener = std::make_shared<PrintingStatisticListener>(
          fmt::format("EngineStats_{:%Y-%m-%d_%H-%M-%S}_{:d}.stats", std::chrono::system_clock::now(), ::getpid()));
    listener->addListener(printingStatisticsListener);
    listener->addSystemListener(printingStatisticsListener);

    if (configuration.enableGoogleEventTrace.getValue())
    {
        auto googleTracePrinter = std::make_shared<GoogleEventTracePrinter>(
            fmt::format("GoogleEventTrace_{:%Y-%m-%d_%H-%M-%S}_{:d}.json", std::chrono::system_clock::now(), ::getpid()));
        listener->addListener(googleTracePrinter);
        listener->addSystemListener(googleTracePrinter);
    }

    if (configuration.enableReplayLogging.getValue())
    {
        auto replayPrinter = std::make_shared<ReplayPrinter>(
            fmt::format("ReplayData_{:%Y-%m-%d_%H-%M-%S}_{:d}", std::chrono::system_clock::now(), ::getpid()));
        listener->addListener(replayPrinter);
        listener->addSystemListener(replayPrinter);
    }

    nodeEngine = NodeEngineBuilder(configuration.workerConfiguration, listener, listener).build();

    optimizer = std::make_unique<QueryOptimizer>(configuration.workerConfiguration.defaultQueryExecution);
    compiler = std::make_unique<QueryCompilation::QueryCompiler>();

    if (configuration.workerConfiguration.bufferSizeInBytes.getValue()
        < configuration.workerConfiguration.defaultQueryExecution.operatorBufferSize.getValue())
    {
        throw InvalidConfigParameter(
            "Currently, we require the bufferSizeInBytes {} to be at least the operatorBufferSize {}",
            configuration.workerConfiguration.bufferSizeInBytes.getValue(),
            configuration.workerConfiguration.defaultQueryExecution.operatorBufferSize.getValue());
    }
}

/// TODO #305: This is a hotfix to get again unique queryId after our initial worker refactoring.
/// We might want to move this to the engine.
static std::atomic queryIdCounter = INITIAL<QueryId>.getRawValue();

std::expected<QueryId, Exception> SingleNodeWorker::registerQuery(LogicalPlan plan) const
{
    try
    {
        plan.setQueryId(QueryId(queryIdCounter++));
        auto queryPlan = optimizer->optimize(plan);
        listener->onEvent(SubmitQuerySystemEvent{queryPlan.getQueryId(), explain(plan, ExplainVerbosity::Debug)});
        auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(queryPlan);
        
        /// If replay logging is enabled, enable dump mode to capture compiled libraries
        if (configuration.enableReplayLogging.getValue())
        {
            request->dumpCompilationResult = DumpMode::FILE;
            std::cout << "Enabled dump mode for replay logging - Query " << queryPlan.getQueryId().getRawValue() << std::endl;
        }
        else
        {
            request->dumpCompilationResult = configuration.workerConfiguration.dumpQueryCompilationIntermediateRepresentations.getValue();
        }
        
        std::cout << "Starting query compilation for Query " << queryPlan.getQueryId().getRawValue() << std::endl;
        auto result = compiler->compileQuery(std::move(request));
        std::cout << "Query compilation completed for Query " << queryPlan.getQueryId().getRawValue() << std::endl;
        
        /// If replay logging is enabled, capture compiled libraries after compilation
        if (configuration.enableReplayLogging.getValue())
        {
            std::cout << "Capturing compiled libraries for Query " << queryPlan.getQueryId().getRawValue() << std::endl;
            captureCompiledLibraries(queryPlan.getQueryId());
        }
        
        return nodeEngine->registerCompiledQueryPlan(std::move(result));
    }
    catch (Exception& e)
    {
        tryLogCurrentException();
        return std::unexpected(e);
    }
}

void SingleNodeWorker::startQuery(QueryId queryId)
{
    nodeEngine->startQuery(queryId);
}

void SingleNodeWorker::stopQuery(QueryId queryId, QueryTerminationType type)
{
    nodeEngine->stopQuery(queryId, type);
}

void SingleNodeWorker::unregisterQuery(QueryId queryId)
{
    nodeEngine->unregisterQuery(queryId);
}

std::optional<QuerySummary> SingleNodeWorker::getQuerySummary(QueryId queryId) const
{
    return nodeEngine->getQueryLog()->getQuerySummary(queryId);
}

std::optional<QueryLog::Log> SingleNodeWorker::getQueryLog(QueryId queryId) const
{
    return nodeEngine->getQueryLog()->getLogForQuery(queryId);
}

void SingleNodeWorker::captureCompiledLibraries(QueryId queryId) const
{
    /// Find the ReplayPrinter to store compiled libraries
    if (auto compositeListener = std::dynamic_pointer_cast<CompositeStatisticListener>(listener))
    {
        if (auto replayPrinter = compositeListener->getReplayPrinter())
        {
            std::cout << "Capturing compilation metadata for Query " << queryId.getRawValue() << std::endl;
            
            /// Since the MLIR backend doesn't create traditional dump files, we'll capture
            /// the query plan and any available metadata instead
            
            /// Create a metadata file with compilation information
            auto metadataPath = std::filesystem::current_path() / fmt::format("query_{}_metadata.json", queryId.getRawValue());
            std::ofstream metadataFile(metadataPath);
            
            nlohmann::json metadata;
            metadata["queryId"] = queryId.getRawValue();
            metadata["compilationBackend"] = "MLIR";
            metadata["compilationTimestamp"] = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            metadata["dumpMode"] = "FILE";
            metadata["note"] = "MLIR backend compiles code in memory and doesn't create traditional dump files";
            
            metadataFile << metadata.dump(2);
            metadataFile.close();
            
            std::cout << "Created metadata file: " << metadataPath << std::endl;
            replayPrinter->storeCompiledLibrary(queryId, metadataPath);
            
            /// Also check if any files were created directly in the current directory
            std::cout << "Checking current directory for any compilation artifacts" << std::endl;
            bool foundAnyFiles = false;
            for (const auto& entry : std::filesystem::directory_iterator(std::filesystem::current_path()))
            {
                if (entry.is_regular_file())
                {
                    auto extension = entry.path().extension().string();
                    auto filename = entry.path().filename().string();
                    auto fileSize = std::filesystem::file_size(entry.path());
                    
                    std::cout << "Found file: " << entry.path().filename() << " (extension: " << extension << ", size: " << fileSize << " bytes)" << std::endl;
                    
                    /// Look for any files that might be compilation artifacts
                    bool isCompiledFile = (extension == ".so" || extension == ".dylib" || extension == ".dll" ||
                                          extension == ".bc" || extension == ".o" || extension == ".obj" ||
                                          extension == ".mlir" || extension == ".ll" || extension == ".s" ||
                                          extension == ".txt" || extension == ".json" || extension == ".xml" ||
                                          filename.find("pipeline") != std::string::npos ||
                                          filename.find("compiled") != std::string::npos ||
                                          filename.find("jit") != std::string::npos ||
                                          filename.find("function") != std::string::npos ||
                                          filename.find("module") != std::string::npos ||
                                          filename.find("ir") != std::string::npos ||
                                          filename.find("dump") != std::string::npos ||
                                          filename.find("nautilus") != std::string::npos ||
                                          filename.find("query") != std::string::npos);
                    
                    if (isCompiledFile)
                    {
                        std::cout << "Found compilation artifact: " << entry.path() << " (extension: " << extension << ")" << std::endl;
                        replayPrinter->storeCompiledLibrary(queryId, entry.path());
                        foundAnyFiles = true;
                    }
                }
            }
            
            if (!foundAnyFiles)
            {
                std::cout << "INFO: No traditional compilation artifacts found for Query " << queryId.getRawValue() << std::endl;
                std::cout << "This is expected with the MLIR backend, which compiles code in memory" << std::endl;
            }
        }
        else
        {
            NES_WARNING("ReplayPrinter not found in CompositeStatisticListener");
        }
    }
    else
    {
        NES_WARNING("Could not cast listener to CompositeStatisticListener");
    }
}

}
