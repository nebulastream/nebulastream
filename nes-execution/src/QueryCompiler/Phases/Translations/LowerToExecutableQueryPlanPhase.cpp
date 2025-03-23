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

#include <Configurations/Worker/PhysicalSourceTypes/BenchmarkSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/KafkaSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/MQTTSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/MemorySourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/MonitoringSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/SenseSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/StaticDataSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/BenchmarkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LambdaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MQTTSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MemorySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MonitoringSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorPlugin.hpp>
#include <Operators/LogicalOperators/Sources/StaticDataSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Exceptions/QueryCompilationException.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <QueryCompiler/Phases/Translations/DefaultDataSourceProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/CompilerConstants.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <string>
#include <utility>
#include <variant>

namespace NES::QueryCompilation {
LowerToExecutableQueryPlanPhase::LowerToExecutableQueryPlanPhase(DataSinkProviderPtr sinkProvider,
                                                                 DataSourceProviderPtr sourceProvider)
    : sinkProvider(std::move(sinkProvider)), sourceProvider(std::move(sourceProvider)){};

LowerToExecutableQueryPlanPhasePtr LowerToExecutableQueryPlanPhase::create(const DataSinkProviderPtr& sinkProvider,
                                                                           const DataSourceProviderPtr& sourceProvider) {
    return std::make_shared<LowerToExecutableQueryPlanPhase>(sinkProvider, sourceProvider);
}

Runtime::Execution::ExecutableQueryPlanPtr LowerToExecutableQueryPlanPhase::apply(const PipelineQueryPlanPtr& pipelineQueryPlan,
                                                                                  const Runtime::NodeEnginePtr& nodeEngine) {
    std::vector<DataSourcePtr> sources;
    std::vector<DataSinkPtr> sinks;
    std::vector<Runtime::Execution::ExecutablePipelinePtr> executablePipelines;
    std::map<PipelineId, Runtime::Execution::SuccessorExecutablePipeline> pipelineToExecutableMap;
    //Process all pipelines recursively.
    auto sourcePipelines = pipelineQueryPlan->getSourcePipelines();
    for (const auto& pipeline : sourcePipelines) {
        processSource(pipeline, sources, sinks, executablePipelines, nodeEngine, pipelineQueryPlan, pipelineToExecutableMap);
    }

    return std::make_shared<Runtime::Execution::ExecutableQueryPlan>(pipelineQueryPlan->getQueryId(),
                                                                     pipelineQueryPlan->getQuerySubPlanId(),
                                                                     pipelineQueryPlan->getQuerySubPlanVersion(),
                                                                     std::move(sources),
                                                                     std::move(sinks),
                                                                     std::move(executablePipelines),
                                                                     nodeEngine->getQueryManager(),
                                                                     nodeEngine->getBufferManager());
}

Runtime::Execution::SuccessorExecutablePipeline LowerToExecutableQueryPlanPhase::processSuccessor(
    const OperatorPipelinePtr& pipeline,
    std::vector<DataSourcePtr>& sources,
    std::vector<DataSinkPtr>& sinks,
    std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
    const Runtime::NodeEnginePtr& nodeEngine,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    std::map<PipelineId, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap) {

    // check if the particular pipeline already exist in the pipeline map.
    if (pipelineToExecutableMap.find(pipeline->getPipelineId()) != pipelineToExecutableMap.end()) {
        return pipelineToExecutableMap.at(pipeline->getPipelineId());
    }

    if (pipeline->isSinkPipeline()) {
        auto executableSink = processSink(pipeline, sources, sinks, executablePipelines, nodeEngine, pipelineQueryPlan);
        pipelineToExecutableMap.insert({pipeline->getPipelineId(), executableSink});
        return executableSink;
    }
    if (pipeline->isOperatorPipeline()) {
        auto executablePipeline = processOperatorPipeline(pipeline,
                                                          sources,
                                                          sinks,
                                                          executablePipelines,
                                                          nodeEngine,
                                                          pipelineQueryPlan,
                                                          pipelineToExecutableMap);
        pipelineToExecutableMap.insert({pipeline->getPipelineId(), executablePipeline});
        return executablePipeline;
    }
    throw QueryCompilationException("The pipeline was of wrong type. It should be a sink pipeline or a operator pipeline");
}

void LowerToExecutableQueryPlanPhase::processSource(
    const OperatorPipelinePtr& pipeline,
    std::vector<DataSourcePtr>& sources,
    std::vector<DataSinkPtr>& sinks,
    std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
    const Runtime::NodeEnginePtr& nodeEngine,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    std::map<PipelineId, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap) {

    if (!pipeline->isSourcePipeline()) {
        NES_ERROR("This is not a source pipeline.");
        NES_ERROR("{}", pipeline->getDecomposedQueryPlan()->toString());
        throw QueryCompilationException("This is not a source pipeline.");
    }

    //Convert logical source descriptor to actual source descriptor
    auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    auto sourceOperator = rootOperator->as<PhysicalOperators::PhysicalSourceOperator>();
    auto sourceDescriptor = sourceOperator->getSourceDescriptor();
    if (sourceDescriptor->instanceOf<LogicalSourceDescriptor>()) {
        //Fetch logical and physical source name in the descriptor
        auto logicalSourceName = sourceDescriptor->getLogicalSourceName();
        auto physicalSourceName = sourceDescriptor->getPhysicalSourceName();
        //Iterate over all available physical sources
        bool foundPhysicalSource = false;
        for (const auto& physicalSourceType : nodeEngine->getPhysicalSourceTypes()) {
            //Check if logical and physical source name matches with any of the physical source provided by the node
            if (physicalSourceType->getLogicalSourceName() == logicalSourceName
                && physicalSourceType->getPhysicalSourceName() == physicalSourceName) {
                sourceDescriptor = createSourceDescriptor(sourceDescriptor->getSchema(), physicalSourceType);
                foundPhysicalSource = true;
                break;
            }
        }
        if (!foundPhysicalSource) {
            throw QueryCompilationException("Unable to find the Physical source with logical source name " + logicalSourceName
                                            + " and physical source name " + physicalSourceName);
        }
    }

    std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessorPipelines;
    for (const auto& successor : pipeline->getSuccessors()) {
        auto executableSuccessor = processSuccessor(successor,
                                                    sources,
                                                    sinks,
                                                    executablePipelines,
                                                    nodeEngine,
                                                    pipelineQueryPlan,
                                                    pipelineToExecutableMap);
        executableSuccessorPipelines.emplace_back(executableSuccessor);
    }

    if (!sourceOperator->hasProperty(LOGICAL_OPERATOR_ID_KEY)) {
        throw QueryCompilationException(
            "Logical operator id not found for the source operator while creating executable query plan.");
    }
    auto logicalOperatorId = std::any_cast<OperatorId>(sourceOperator->getProperty(LOGICAL_OPERATOR_ID_KEY));
    auto source = sourceProvider->lower(logicalOperatorId,
                                        sourceOperator->getOriginId(),
                                        sourceOperator->getStatisticId(),
                                        sourceDescriptor,
                                        nodeEngine,
                                        executableSuccessorPipelines);

    // Add this source as a predecessor to the pipeline execution context's of all its children.
    // This way you can navigate upstream.
    for (auto executableSuccessor : executableSuccessorPipelines) {
        if (const auto* nextExecutablePipeline = std::get_if<Runtime::Execution::ExecutablePipelinePtr>(&executableSuccessor)) {
            NES_DEBUG("Adding current source operator: {} as a predecessor to its child pipeline: {}",
                      source->getOperatorId(),
                      (*nextExecutablePipeline)->getPipelineId());
            (*nextExecutablePipeline)->getContext()->addPredecessor(source);
        }
        // note: we do not register predecessors for DataSinks.
    }
    sources.emplace_back(source);
}

Runtime::Execution::SuccessorExecutablePipeline
LowerToExecutableQueryPlanPhase::processSink(const OperatorPipelinePtr& pipeline,
                                             std::vector<DataSourcePtr>&,
                                             std::vector<DataSinkPtr>& sinks,
                                             std::vector<Runtime::Execution::ExecutablePipelinePtr>&,
                                             Runtime::NodeEnginePtr nodeEngine,
                                             const PipelineQueryPlanPtr& pipelineQueryPlan) {
    auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    auto sinkOperator = rootOperator->as<PhysicalOperators::PhysicalSinkOperator>();
    auto numOfProducers = pipeline->getPredecessors().size();
    auto sink = sinkProvider->lower(sinkOperator->getId(),
                                    sinkOperator->getSinkDescriptor(),
                                    sinkOperator->getOutputSchema(),
                                    std::move(nodeEngine),
                                    pipelineQueryPlan,
                                    numOfProducers);
    // propagate marker from logical sink operator to executable
    auto migrationFlagVal = sinkOperator->getProperty(MIGRATION_SINK);
    if (migrationFlagVal.has_value() && std::any_cast<bool>(migrationFlagVal)) {
        sink->setMigrationFlag();
    }
    sinks.emplace_back(sink);
    return sink;
}

Runtime::Execution::SuccessorExecutablePipeline LowerToExecutableQueryPlanPhase::processOperatorPipeline(
    const OperatorPipelinePtr& pipeline,
    std::vector<DataSourcePtr>& sources,
    std::vector<DataSinkPtr>& sinks,
    std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
    const Runtime::NodeEnginePtr& nodeEngine,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    std::map<PipelineId, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap) {

    auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    auto executableOperator = rootOperator->as<ExecutableOperator>();

    std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessorPipelines;
    for (const auto& successor : pipeline->getSuccessors()) {
        auto executableSuccessor = processSuccessor(successor,
                                                    sources,
                                                    sinks,
                                                    executablePipelines,
                                                    nodeEngine,
                                                    pipelineQueryPlan,
                                                    pipelineToExecutableMap);
        executableSuccessorPipelines.emplace_back(executableSuccessor);
    }

    auto queryManager = nodeEngine->getQueryManager();

    auto emitToSuccessorFunctionHandler = [executableSuccessorPipelines](Runtime::TupleBuffer& buffer,
                                                                         Runtime::WorkerContextRef workerContext) {
        for (const auto& executableSuccessor : executableSuccessorPipelines) {
            if (const auto* sink = std::get_if<DataSinkPtr>(&executableSuccessor)) {
                NES_TRACE("Emit Buffer to data sink {}", (*sink)->toString());
                (*sink)->writeData(buffer, workerContext);
            } else if (const auto* nextExecutablePipeline =
                           std::get_if<Runtime::Execution::ExecutablePipelinePtr>(&executableSuccessor)) {
                NES_TRACE("Emit Buffer to pipeline {}", (*nextExecutablePipeline)->getPipelineId());
                (*nextExecutablePipeline)->execute(buffer, workerContext);
            }
        }
    };

    auto emitToQueryManagerFunctionHandler = [executableSuccessorPipelines, queryManager](Runtime::TupleBuffer& buffer) {
        for (const auto& executableSuccessor : executableSuccessorPipelines) {
            NES_TRACE("Emit buffer to query manager");
            queryManager->addWorkForNextPipeline(buffer, executableSuccessor);
        }
    };

    // Check if pipeline is for migration and one of successor pipelines is sink that should be used for migration
    std::optional<std::function<void(Runtime::TupleBuffer&, Runtime::WorkerContext&)>> emitMigrationHandler = nullptr;
    if (pipeline->isMigrationPipeline()
        && std::any_of(executableSuccessorPipelines.begin(),
                       executableSuccessorPipelines.end(),
                       [](Runtime::Execution::SuccessorExecutablePipeline pipeline) {
                           if (const auto* sink = std::get_if<DataSinkPtr>(&pipeline)) {
                               return (*sink)->isForMigration();
                           }
                           return false;
                       })) {

        // Add migration handler that should be used for migration of the state.
        // It sends migration buffers only to sinks responsible for migration
        emitMigrationHandler = [executableSuccessorPipelines](Runtime::TupleBuffer& buffer,
                                                              Runtime::WorkerContextRef workerContext) {
            for (const auto& executableSuccessor : executableSuccessorPipelines) {
                if (const auto* sink = std::get_if<DataSinkPtr>(&executableSuccessor)) {
                    if ((*sink)->isForMigration()) {
                        NES_DEBUG("Emit Buffer to data migration sink {}", (*sink)->toString());
                        (*sink)->writeData(buffer, workerContext);
                    }
                }
            }
        };
    }

    auto executionContext =
        std::make_shared<Runtime::Execution::PipelineExecutionContext>(pipeline->getPipelineId(),
                                                                       pipelineQueryPlan->getQuerySubPlanId(),
                                                                       queryManager->getBufferManager(),
                                                                       queryManager->getNumberOfWorkerThreads(),
                                                                       emitToSuccessorFunctionHandler,
                                                                       emitToQueryManagerFunctionHandler,
                                                                       executableOperator->getOperatorHandlers(),
                                                                       emitMigrationHandler);

    auto executablePipeline = Runtime::Execution::ExecutablePipeline::create(pipeline->getPipelineId(),
                                                                             pipelineQueryPlan->getQueryId(),
                                                                             pipelineQueryPlan->getQuerySubPlanId(),
                                                                             pipelineQueryPlan->getQuerySubPlanVersion(),
                                                                             queryManager,
                                                                             executionContext,
                                                                             executableOperator->getExecutablePipelineStage(),
                                                                             pipeline->getPredecessors().size(),
                                                                             executableSuccessorPipelines,
                                                                             false,// reconfiguration
                                                                             pipeline->isMigrationPipeline());

    // Add this pipeline as a predecessor to the pipeline execution context's of all its children.
    // This way you can navigate upstream.
    for (auto executableSuccessor : executableSuccessorPipelines) {
        if (const auto* nextExecutablePipeline = std::get_if<Runtime::Execution::ExecutablePipelinePtr>(&executableSuccessor)) {
            NES_DEBUG("Adding current pipeline: {} as a predecessor to its child pipeline: {}",
                      executablePipeline->getPipelineId(),
                      (*nextExecutablePipeline)->getPipelineId());
            (*nextExecutablePipeline)->getContext()->addPredecessor(executablePipeline);
        }
        // note: we do not register predecessors for DataSinks.
    }

    executablePipelines.emplace_back(executablePipeline);
    return executablePipeline;
}

SourceDescriptorPtr LowerToExecutableQueryPlanPhase::createSourceDescriptor(SchemaPtr schema,
                                                                            PhysicalSourceTypePtr physicalSourceType) {
    auto logicalSourceName = physicalSourceType->getLogicalSourceName();
    auto physicalSourceName = physicalSourceType->getPhysicalSourceName();
    auto sourceType = physicalSourceType->getSourceType();
    NES_DEBUG("PhysicalSourceConfig: create Actual source descriptor with physical source: {} {} ",
              physicalSourceType->toString(),
              magic_enum::enum_name(sourceType));

    switch (sourceType) {
        case SourceType::DEFAULT_SOURCE: {
            auto defaultSourceType = physicalSourceType->as<DefaultSourceType>();
            return DefaultSourceDescriptor::create(
                schema,
                logicalSourceName,
                defaultSourceType->getNumberOfBuffersToProduce()->getValue(),
                std::chrono::milliseconds(defaultSourceType->getSourceGatheringInterval()->getValue()).count());
        }
#ifdef ENABLE_MQTT_BUILD
        case SourceType::MQTT_SOURCE: {
            auto mqttSourceType = physicalSourceType->as<MQTTSourceType>();
            return MQTTSourceDescriptor::create(schema, mqttSourceType);
        }
#endif
        case SourceType::CSV_SOURCE: {
            auto csvSourceType = physicalSourceType->as<CSVSourceType>();
            return CsvSourceDescriptor::create(schema, csvSourceType, logicalSourceName, physicalSourceName);
        }
        case SourceType::SENSE_SOURCE: {
            auto senseSourceType = physicalSourceType->as<SenseSourceType>();
            return SenseSourceDescriptor::create(schema, logicalSourceName, senseSourceType->getUdfs()->getValue());
        }
        case SourceType::MEMORY_SOURCE: {
            auto memorySourceType = physicalSourceType->as<MemorySourceType>();
            return MemorySourceDescriptor::create(schema,
                                                  memorySourceType->getMemoryArea(),
                                                  memorySourceType->getMemoryAreaSize(),
                                                  memorySourceType->getNumberOfBufferToProduce(),
                                                  memorySourceType->getGatheringValue(),
                                                  memorySourceType->getGatheringMode(),
                                                  memorySourceType->getSourceAffinity(),
                                                  memorySourceType->getTaskQueueId(),
                                                  logicalSourceName,
                                                  physicalSourceName);
        }
        case SourceType::MONITORING_SOURCE: {
            auto monitoringSourceType = physicalSourceType->as<MonitoringSourceType>();
            return MonitoringSourceDescriptor::create(
                monitoringSourceType->getWaitTime(),
                Monitoring::MetricCollectorType(monitoringSourceType->getMetricCollectorType()));
        }
        case SourceType::BENCHMARK_SOURCE: {
            auto benchmarkSourceType = physicalSourceType->as<BenchmarkSourceType>();
            return BenchmarkSourceDescriptor::create(schema,
                                                     benchmarkSourceType->getMemoryArea(),
                                                     benchmarkSourceType->getMemoryAreaSize(),
                                                     benchmarkSourceType->getNumberOfBuffersToProduce(),
                                                     benchmarkSourceType->getGatheringValue(),
                                                     benchmarkSourceType->getGatheringMode(),
                                                     benchmarkSourceType->getSourceMode(),
                                                     benchmarkSourceType->getSourceAffinity(),
                                                     benchmarkSourceType->getTaskQueueId(),
                                                     logicalSourceName,
                                                     physicalSourceName);
        }
        case SourceType::STATIC_DATA_SOURCE: {
            auto staticDataSourceType = physicalSourceType->as<NES::Experimental::StaticDataSourceType>();
            return NES::Experimental::StaticDataSourceDescriptor::create(schema,
                                                                         staticDataSourceType->getPathTableFile(),
                                                                         staticDataSourceType->getLateStart());
        }
        case SourceType::LAMBDA_SOURCE: {
            auto lambdaSourceType = physicalSourceType->as<LambdaSourceType>();
            return LambdaSourceDescriptor::create(schema,
                                                  lambdaSourceType->getGenerationFunction(),
                                                  lambdaSourceType->getNumBuffersToProduce(),
                                                  lambdaSourceType->getGatheringValue(),
                                                  lambdaSourceType->getGatheringMode(),
                                                  lambdaSourceType->getSourceAffinity(),
                                                  lambdaSourceType->getTaskQueueId(),
                                                  logicalSourceName,
                                                  physicalSourceName);
        }
        case SourceType::TCP_SOURCE: {
            auto tcpSourceType = physicalSourceType->as<TCPSourceType>();
            return TCPSourceDescriptor::create(schema, tcpSourceType, logicalSourceName, physicalSourceName);
        }
        case SourceType::KAFKA_SOURCE: {
            auto kafkaSourceType = physicalSourceType->as<KafkaSourceType>();
            return KafkaSourceDescriptor::create(schema,
                                                 kafkaSourceType->getBrokers()->getValue(),
                                                 logicalSourceName,
                                                 kafkaSourceType->getTopic()->getValue(),
                                                 kafkaSourceType->getGroupId()->getValue(),
                                                 kafkaSourceType->getAutoCommit()->getValue(),
                                                 kafkaSourceType->getConnectionTimeout()->getValue(),
                                                 kafkaSourceType->getOffsetMode()->getValue(),
                                                 kafkaSourceType,
                                                 kafkaSourceType->getNumberOfBuffersToProduce()->getValue(),
                                                 kafkaSourceType->getBatchSize()->getValue());
        }
        default: {
            // check if a plugin can create the correct source descriptor
            for (const auto& plugin : SourceDescriptorPluginRegistry::getPlugins()) {
                auto descriptor = plugin->create(schema, physicalSourceType);
                if (descriptor != nullptr) {
                    return descriptor;
                }
            }
            throw QueryCompilationException("PhysicalSourceConfig:: source type " + physicalSourceType->getSourceTypeAsString()
                                            + " not supported");
        }
    }
}

}// namespace NES::QueryCompilation
