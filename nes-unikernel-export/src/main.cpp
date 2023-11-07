#include "NoOpPhysicalSource.h"
#include "Runtime/NodeEngine.hpp"
#include "Services/QueryService.hpp"
#include "UnikernelExport.h"
#include <CLIOptions.h>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <ExportQueryOptimizer.h>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <QueryCompiler.h>
#include <Runtime/BufferManager.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlacementStrategy.hpp>
#include <YamlExport.h>
#include <iostream>
#include <memory>
#include <ranges>

class NESProvider {
    NES::Configurations::CoordinatorConfigurationPtr config = std::make_shared<NES::Configurations::CoordinatorConfiguration>();
    QueryParsingServicePtr queryParsingService = QueryParsingService::create(
        Compiler::JITCompilerBuilder().registerLanguageCompiler(Compiler::CPPCompiler::create()).build());
    NES::TopologyPtr topology;
    NES::Catalogs::Source::SourceCatalogPtr sourceCatalog;
    NES::Runtime::NodeEngine nodeEngine;
    ExportQueryOptimizer optimizer;

  public:
    NESProvider(std::vector<NES::PhysicalSourceTypePtr> physicalSources,
                NES::TopologyPtr topology,
                NES::Catalogs::Source::SourceCatalogPtr sourceCatalog)
        : topology(topology), sourceCatalog(sourceCatalog),
          nodeEngine(std::move(physicalSources), {std::make_shared<NES::Runtime::BufferManager>()}) {}

    NES::QueryPlanPtr parseQueryString(const std::string& query) { return queryParsingService->createQueryFromCodeString(query); }
    auto optimize(NES::QueryPlanPtr queryPlan) { return optimizer.optimize(topology, config, queryPlan, sourceCatalog); }
};

bool isUnikernelWorkerNode(const NES::ExecutionNodePtr node) {
    auto subplans = node->getQuerySubPlans(1)[0];
    auto sink = subplans->getRootOperators()[0]->as<SinkLogicalOperatorNode>();
    auto sources = subplans->getOperatorByType<SourceLogicalOperatorNode>();
    bool hasNetworkSink = sink->getSinkDescriptor()->instanceOf<NES::Network::NetworkSinkDescriptor>();
    bool hasOnlyNetworkSources = std::ranges::all_of(sources, [](const SourceLogicalOperatorNodePtr& source) {
        return source->getSourceDescriptor()->instanceOf<NES::Network::NetworkSourceDescriptor>();
    });
    NES_DEBUG("{} is {} Unikernel Worker", node->toString(), (hasNetworkSink && hasOnlyNetworkSources) ? "a" : "not a");
    return hasNetworkSink && hasOnlyNetworkSources;
}

int main(int argc, char** argv) {
    using namespace NES;
    using namespace NES::Runtime;
    namespace stdr = std::ranges;
    namespace stdv = std::ranges::views;

    NES::Logger::setupLogging("unikernel_export.log", NES::LogLevel::LOG_DEBUG);
    auto cliResult = Options::getCLIOptions(argc, argv);

    if (!cliResult) {
        NES_FATAL_ERROR("When Parsing CLI Arguments: {}", cliResult.error());
        return 1;
    }

    auto options = cliResult.value();
    YamlExport yamlExport;

    auto [topology, sourcesCatalog, physicalSources] = options.getTopologyAndSources();
    NESProvider nesProvider(physicalSources, topology, sourcesCatalog);
    NES_DEBUG("Exporter Starter");
    auto query = nesProvider.parseQueryString(options.getQueryString());
    auto [queryPlan, gep] = nesProvider.optimize(query);

    NES_INFO("Topology: \n{}", topology->toString());
    NES_INFO("GEP:\n{}", gep->getAsString());

    yamlExport.setQueryPlan(queryPlan, gep);

    ExportQueryCompiler queryCompiler;

    for (const auto& executionNode : gep->getExecutionNodesByQueryId(1) | stdv::filter(isUnikernelWorkerNode)) {
        auto subQueries = executionNode->getQuerySubPlans(1);
        std::vector<WorkerSubQuery> workerSubQueries;
        for (const auto& subquery : subQueries) {
            auto logicalSubQueryPlan = subquery->copy();
            auto [sinkMap, sourceMap, stages, sharedHandler] = queryCompiler.compile(subquery, options.getBufferSize());
            UnikernelExport unikernelExport;
            std::vector<WorkerSubQueryStage> stageIds;

            if (!sharedHandler.empty()) {
                unikernelExport.exportSharedOperatorHandlersToObjectFile(
                    options.getOutputPathForFile(fmt::format("sharedHandlers{}.o", subquery->getQuerySubPlanId())),
                    subquery->getQuerySubPlanId(),
                    sharedHandler);
            }

            for (auto& [pipelineId, successor, predecessors, stage, handlers] : stdv::reverse(stages)) {
                stageIds.emplace_back(pipelineId, predecessors, successor, handlers.size());
                unikernelExport.exportPipelineStageToObjectFile(
                    options.getOutputPathForFile("stage" + std::to_string(pipelineId) + ".o"),
                    pipelineId,
                    subquery->getQuerySubPlanId(),
                    std::move(handlers),
                    std::move(stage));
            }
            workerSubQueries.emplace_back(logicalSubQueryPlan, stageIds, sourceMap, sinkMap);
        }
        yamlExport.addWorker(workerSubQueries, executionNode);
    }
    yamlExport.writeToOutputFile(options.getYAMLOutputPath());
}