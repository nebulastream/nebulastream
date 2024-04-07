#include <CLIOptions.h>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <ExportQueryOptimizer.h>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryPlanExport.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Services/QueryParsingService.hpp>
#include <Stage/CppBackendExport.hpp>
#include <Stage/OperatorHandlerCppExport.hpp>
#include <Stage/QueryPipeliner.hpp>
#include <Util/Logger/Logger.hpp>
#include <YamlExport.h>
#include <iostream>
#include <memory>
#include <ranges>

class NESProvider {
    NES::Configurations::CoordinatorConfigurationPtr config = std::make_shared<NES::Configurations::CoordinatorConfiguration>();
    std::shared_ptr<QueryParsingService> queryParsingService = QueryParsingService::create(
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

bool isUnikernelWorkerNode(const NES::Optimizer::ExecutionNodeWLock lockedNode) {
    auto node = lockedNode->operator*();
    auto subplans = node->getAllDecomposedQueryPlans(1)[0];
    auto sink = subplans->getRootOperators()[0]->as<SinkLogicalOperator>();
    auto sources = subplans->getOperatorByType<SourceLogicalOperator>();
    bool hasNetworkSink = sink->getSinkDescriptor()->instanceOf<NES::Network::NetworkSinkDescriptor>();
    NES_DEBUG("{} is {} Unikernel Worker", node->toString(), (hasNetworkSink) ? "a" : "not a");
    return hasNetworkSink;
}

struct OutputFileEmitter {
    boost::filesystem::path outputDirectory;

    void emitFile(const std::string& filename, const std::string& content) {
        boost::filesystem::ofstream os(outputDirectory / filename);
        NES_ASSERT(os.good(), fmt::format("Could not open output file {}", (outputDirectory / filename).string()));
        os << content;
    }
};

int main(int argc, char** argv) {
    using namespace NES;
    using namespace NES::Runtime;
    namespace stdr = std::ranges;
    namespace stdv = std::ranges::views;

    NES::Logger::setupLogging("unikernel_export.log", NES::LogLevel::LOG_INFO);
    auto cliResult = Options::getCLIOptions(argc, argv);

    if (!cliResult) {
        NES_FATAL_ERROR("When Parsing CLI Arguments: {}", cliResult.error());
        return 1;
    }

    auto options = cliResult.value();
    YamlExport yamlExport(options.useKafka() ? std::make_optional<>(options.getKafkaConfiguration()) : std::nullopt);

    auto [topology, sourcesCatalog, physicalSources] = options.getTopologyAndSources();
    topology->print();
    NESProvider nesProvider(physicalSources, topology, sourcesCatalog);
    NES_DEBUG("Exporter Starter");
    auto query = nesProvider.parseQueryString(options.getQueryString());
    auto [queryPlan, gep] = nesProvider.optimize(query);

    NES_INFO("Topology: \n{}", topology->toString());
    NES_INFO("GEP:\n{}", gep->getAsString());

    yamlExport.setQueryPlan(queryPlan, topology, gep, sourcesCatalog);

    NES::Unikernel::Export::QueryPlanExporter queryPlanExporter(sourcesCatalog);

    for (const auto& lockedExecutionNode :
         gep->getLockedExecutionNodesHostingSharedQueryId(1) | stdv::filter(isUnikernelWorkerNode)) {
        auto executionNode = lockedExecutionNode->operator*();
        auto subQueries = executionNode->getAllDecomposedQueryPlans(1);
        auto topologyNode = topology->lockTopologyNode(executionNode->getId());
        std::vector<WorkerSubQuery> workerSubQueries;
        for (const auto& subquery : subQueries) {
            auto logicalSubQueryPlan = subquery->copy();
            auto pipeliner = NES::Unikernel::Export::QueryPipeliner(8192);
            auto pipeliningResult = pipeliner.lowerQuery(subquery);
            auto cppEmitter = NES::Unikernel::Export::CppBackendExport();
            OutputFileEmitter emitter((options.getStageOutputPathForNode(executionNode->getId())));

            if (!pipeliningResult.sharedOperatorHandler.empty()) {
                auto handler = NES::Unikernel::Export::OperatorHandlerCppExporter::generateSharedHandler(
                    pipeliningResult.sharedOperatorHandler,
                    subquery->getDecomposedQueryPlanId());
                emitter.emitFile(fmt::format("sharedHandler{}.cpp", subquery->getDecomposedQueryPlanId()), handler);
            }

            auto isNautilusStage = std::ranges::views::filter([](const NES::Unikernel::Export::Stage& stage) {
                return stage.pipeline->isOperatorPipeline();
            });

            for (const auto& stage : pipeliningResult.stages | isNautilusStage) {
                std::stringstream ss;
                ss << cppEmitter.emitAsCppSourceFiles(stage);
                ss << NES::Unikernel::Export::OperatorHandlerCppExporter::generateHandler(stage,
                                                                                          subquery->getDecomposedQueryPlanId());
                emitter.emitFile(fmt::format("stage{}.cpp", stage.pipeline->getPipelineId()), ss.str());
            }

            auto queryPlanExporterResult = queryPlanExporter.exportQueryPlan(pipeliningResult);

            workerSubQueries.emplace_back(logicalSubQueryPlan, pipeliningResult, queryPlanExporterResult);
        }
        yamlExport.addWorker(workerSubQueries, executionNode, topologyNode->operator*());
    }
    yamlExport.writeToOutputFile(options.getYAMLOutputPath());
}