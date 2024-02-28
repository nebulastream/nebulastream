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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <NoOp/NoOpPhysicalSourceType.hpp>
#include <NoOp/NoOpSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <YamlExport.h>
#include <ranges>

static std::optional<NES::ExecutionNodePtr> findNodeByOperator(const NES::SourceLogicalOperatorNodePtr& sourceOp,
                                                               const NES::GlobalExecutionPlanPtr& gep) {
    auto nodeWithOperator = std::ranges::views::filter([&sourceOp](const NES::ExecutionNodePtr& node) {
        auto sources = node->getQuerySubPlans(1)[0]->getOperatorByType<NES::SourceLogicalOperatorNode>();
        for (const auto& source : sources) {
            if (source->getId() == sourceOp->getId()) {
                return true;
            }
        }
        return false;
    });

    for (const auto& node : gep->getAllExecutionNodes() | nodeWithOperator | std::ranges::views::take(1)) {
        return node;
    }
    NES_ERROR("Could not find Operator {}", sourceOp->getId());
    return std::nullopt;
}

void YamlExport::setQueryPlan(NES::QueryPlanPtr queryPlan,
                              NES::GlobalExecutionPlanPtr gep,
                              NES::Catalogs::Source::SourceCatalogPtr sourceCatalog) {
    auto sourceOperators = queryPlan->getSourceOperators();
    std::vector<SourceEndpointConfiguration> sources;

    std::ranges::transform(
        sourceOperators,
        std::back_inserter(sources),
        [&gep, &sourceCatalog](const NES::SourceLogicalOperatorNodePtr& sourceOp) {
            SourceEndpointConfiguration source;
            auto sourceNode = findNodeByOperator(sourceOp, gep).value();
            if (sourceOp->getSourceDescriptor()->instanceOf<NES::Network::NetworkSourceDescriptor>()) {
                source.schema = SchemaConfiguration(MANUAL, sourceOp->getOutputSchema());
                source.subQueryID = sourceNode->getQuerySubPlans(1)[0]->getQuerySubPlanId();
                source.nodeId = sourceNode->getTopologyNode()->getId();
                source.ip = sourceNode->getTopologyNode()->getIpAddress();
                source.port = sourceNode->getTopologyNode()->getDataPort();
                source.originId = sourceOp->getOriginId();
                source.type = NetworkSource;
            } else {
                auto physicalSource =
                    *sourceCatalog->getPhysicalSources(sourceOp->getSourceDescriptor()->getLogicalSourceName()).begin();
                auto sourceType = physicalSource->getPhysicalSource()->getPhysicalSourceType();
                auto noOpSourceType = std::dynamic_pointer_cast<NES::NoOpPhysicalSourceType>(sourceType);

                source.schema = SchemaConfiguration(noOpSourceType->getSchemaType(), sourceOp->getOutputSchema());
                source.ip = noOpSourceType->getTCP()->ip;
                source.port = noOpSourceType->getTCP()->port;
                source.format = noOpSourceType->getTCP()->format;
                source.type = TcpSource;
            }

            return source;
        });

    auto sink = queryPlan->getSinkOperators();
    assert(sink.size() == 1);
    auto sinkSchema = sink[0]->getOutputSchema();
    this->setSinkSchema(sinkSchema);

    this->configuration.query.queryID = queryPlan->getQueryId();
    this->configuration.query.workerID = 1;

    //Assumption: Sink 1 Worker 2 Source 3
    auto sinkNode = gep->getExecutionNodeByNodeId(1);
    this->configuration.sink.nodeId = sinkNode->getTopologyNode()->getId();
    this->configuration.sink.subQueryID = sinkNode->getQuerySubPlans(1)[0]->getQuerySubPlanId();
    this->configuration.sink.operatorId =
        sinkNode->getQuerySubPlans(1)[0]->getOperatorByType<NES::SourceLogicalOperatorNode>()[0]->getId();
    this->configuration.sink.ip = sinkNode->getTopologyNode()->getIpAddress();
    this->configuration.sink.port = sinkNode->getTopologyNode()->getDataPort();

    this->configuration.sources = std::move(sources);
}

WorkerTCPSourceConfiguration buildTCPSource(const NES::NoOpSourceDescriptor& desc) {
    return WorkerTCPSourceConfiguration{desc.getTcp()->ip,
                                        desc.getTcp()->port,
                                        desc.getTcp()->format,
                                        SchemaConfiguration(MANUAL, desc.getSchema())};
}

WorkerLinkConfiguration buildWorkerLink(NES::Network::NetworkSourceDescriptorPtr desc) {
    return WorkerLinkConfiguration{desc->getNodeLocation().getHostname(),
                                   desc->getNodeLocation().getPort(),
                                   desc->getNodeLocation().getNodeId(),
                                   desc->getNesPartition().getPartitionId(),
                                   desc->getNesPartition().getSubpartitionId(),
                                   desc->getNesPartition().getOperatorId()

    };
}

WorkerSourceConfiguration buildSource(const NES::Unikernel::Export::QueryPlanExporter::ExportSourceDescriptor& source) {
    if (source.sourceDescriptor->instanceOf<NES::Network::NetworkSourceDescriptor>()) {
        auto networkSource = source.sourceDescriptor->as<NES::Network::NetworkSourceDescriptor>();
        return WorkerSourceConfiguration{
            networkSource->getNesPartition().getOperatorId(),
            source.originId,
            buildWorkerLink(networkSource),
            std::nullopt,
        };
    } else if (source.sourceDescriptor->instanceOf<NES::NoOpSourceDescriptor>()) {
        auto noOpSource = source.sourceDescriptor->as<NES::NoOpSourceDescriptor>();
        NES_ASSERT(noOpSource->getTcp().has_value(), "TCP Configuration is missing in Source Descriptor");
        return WorkerSourceConfiguration{noOpSource->getOperatorId(), source.originId, std::nullopt, buildTCPSource(*noOpSource)};
    }

    NES_NOT_IMPLEMENTED();
}

WorkerStageConfiguration buildTreeRec(
    const NES::Unikernel::Export::Stage& current,
    const std::unordered_map<NES::PipelineId, NES::Unikernel::Export::Stage>& stages,
    const std::unordered_map<NES::PipelineId, NES::Unikernel::Export::QueryPlanExporter::ExportSourceDescriptor>& sources) {
    WorkerStageConfiguration config;
    config.stageId = current.pipeline->getPipelineId();
    config.numberOfOperatorHandlers = current.handler.size();

    auto nonSources = std::ranges::views::filter([sources](auto pipelineId) {
        return !sources.contains(pipelineId);
    });

    std::vector<WorkerStageConfiguration> predecessor;
    std::ranges::transform(current.predecessors | nonSources,
                           std::back_inserter(predecessor),
                           [&stages, &sources](NES::PipelineId pipelineId) -> WorkerStageConfiguration {
                               return buildTreeRec(stages.at(pipelineId), stages, sources);
                           });

    bool eitherOne = false;
    if (!predecessor.empty()) {
        eitherOne = true;
        config.predecessor.emplace(predecessor);
    }

    for (const auto& pred : current.predecessors) {
        if (sources.contains(pred)) {
            eitherOne = true;
            config.upstream = buildSource(sources.at(pred));
        }
    }

    NES_ASSERT(eitherOne, "Either Predecessor Stage or Source");
    return config;
}

WorkerStageConfiguration
buildTree(const NES::Unikernel::Export::QueryPlanExporter::ExportSinkDescriptor& sink,
          const std::vector<NES::Unikernel::Export::Stage>& stages,
          const std::unordered_map<NES::PipelineId, NES::Unikernel::Export::QueryPlanExporter::ExportSourceDescriptor>& sources) {
    std::unordered_map<NES::PipelineId, NES::Unikernel::Export::Stage> stageMap;
    for (const auto& stage : stages) {
        stageMap[stage.pipeline->getPipelineId()] = stage;
    }
    return buildTreeRec(stageMap[sink.predecessor[0]], stageMap, sources);
}

void YamlExport::addWorker(const std::vector<WorkerSubQuery>& subQueries, const NES::ExecutionNodePtr& workerNode) {
    std::vector<WorkerSubQueryConfiguration> subQueryConfiguration;
    std::ranges::transform(subQueries, std::back_inserter(subQueryConfiguration), [this](const WorkerSubQuery& subQuery) {
        auto sink = subQuery.subplan->getOperatorByType<NES::SinkLogicalOperatorNode>();
        NES_ASSERT2_FMT(sink.size() == 1, "Expected Single Sink: {}", subQuery.subplan->toString());
        auto networkSinkDescriptor = sink[0]->getSinkDescriptor()->as<NES::Network::NetworkSinkDescriptor>();
        auto type = WorkerDownStreamLinkConfigurationType::node;
        std::optional<KafkaSinkConfiguration> kafkaSinkConfig = std::nullopt;
        std::optional<WorkerLinkConfiguration> workerLink = std::nullopt;
        if (exportToKafka.has_value() && networkSinkDescriptor->getNodeLocation().getNodeId() == SINK_NODE) {
            type = WorkerDownStreamLinkConfigurationType::kafka;
            kafkaSinkConfig.emplace(KafkaSinkConfiguration{SchemaConfiguration(MANUAL, sink[0]->getOutputSchema()),
                                                           exportToKafka->broker,
                                                           exportToKafka->topic});
        } else {
            type = WorkerDownStreamLinkConfigurationType::node;
            workerLink.emplace(WorkerLinkConfiguration{
                networkSinkDescriptor->getNodeLocation().getHostname(),
                networkSinkDescriptor->getNodeLocation().getPort(),
                networkSinkDescriptor->getNodeLocation().getNodeId(),
                networkSinkDescriptor->getNesPartition().getPartitionId(),
                networkSinkDescriptor->getNesPartition().getSubpartitionId(),
                networkSinkDescriptor->getNesPartition().getOperatorId(),
            });
        }

        NES_ASSERT2_FMT(subQuery.sourcesAndSinks.sinksByPipeline.size() == 1, "Expected exactly one Sink");
        NES_ASSERT2_FMT(subQuery.sourcesAndSinks.sinksByPipeline.begin()->second.predecessor.size() == 1,
                        "Sink Pipeline to have a single predecessor");
        auto rootPipeline = subQuery.sourcesAndSinks.sinksByPipeline.begin()->second.predecessor[0];
        if (subQuery.sourcesAndSinks.sourcesByPipeline.count(rootPipeline)) {
            return WorkerSubQueryConfiguration{std::nullopt,
                                               buildSource(subQuery.sourcesAndSinks.sourcesByPipeline.at(rootPipeline)),
                                               subQuery.subplan->getQuerySubPlanId(),
                                               sink[0]->getOutputSchema()->getSchemaSizeInBytes(),
                                               type,
                                               workerLink,
                                               kafkaSinkConfig};
        }

        return WorkerSubQueryConfiguration{buildTree(subQuery.sourcesAndSinks.sinksByPipeline.begin()->second,
                                                     subQuery.stages.stages,
                                                     subQuery.sourcesAndSinks.sourcesByPipeline),
                                           std::nullopt,
                                           subQuery.subplan->getQuerySubPlanId(),
                                           sink[0]->getOutputSchema()->getSchemaSizeInBytes(),
                                           type,
                                           workerLink,
                                           kafkaSinkConfig};
    });

    this->configuration.workers.emplace_back(workerNode->getTopologyNode()->getIpAddress(),
                                             workerNode->getTopologyNode()->getDataPort(),
                                             workerNode->getTopologyNode()->getId(),
                                             subQueryConfiguration);
}
void YamlExport::setSinkSchema(const NES::SchemaPtr& schema) {
    configuration.sink.schema.fields.clear();
    for (const auto& field : schema->fields) {
        configuration.sink.schema.fields.emplace_back(field->getName(),
                                                      std::string(magic_enum::enum_name(toBasicType(field->getDataType()))));
    }
}
void YamlExport::writeToOutputFile(std::string filepath) const {
    std::ofstream f(filepath);
    YAML::Node yaml;
    yaml = this->configuration;
    NES_INFO("Writing Export YAML to {}", filepath);
    f << yaml;
}

YamlExport::YamlExport(const std::optional<ExportKafkaConfiguration>& exportToKafka) : exportToKafka(exportToKafka) {}
