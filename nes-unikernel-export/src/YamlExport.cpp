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

#include <NoOpSourceDescriptor.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <YamlExport.h>
#include <ranges>

static std::optional<NES::ExecutionNodePtr> findNodeByOperator(const NES::SourceLogicalOperatorNodePtr &sourceOp,
                                                               const NES::GlobalExecutionPlanPtr &gep) {
    auto nodeWithOperator = std::ranges::views::filter([&sourceOp](const NES::ExecutionNodePtr &node) {
        auto sources = node->getQuerySubPlans(1)[0]->getOperatorByType<NES::SourceLogicalOperatorNode>();
        for (const auto &source: sources) {
            if (source->getId() == sourceOp->getId()) {
                return true;
            }
        }
        return false;
    });

    for (const auto &node: gep->getAllExecutionNodes() | nodeWithOperator | std::ranges::views::take(1)) {
        return node;
    }
    NES_ERROR("Could not find Operator {}", sourceOp->getId());
    return std::nullopt;
}

void YamlExport::setQueryPlan(NES::QueryPlanPtr queryPlan, NES::GlobalExecutionPlanPtr gep) {
    auto sourceOperators = queryPlan->getSourceOperators();
    std::vector<EndpointConfiguration> sources;

    std::ranges::transform(sourceOperators,
                           std::back_inserter(sources),
                           [&gep](const NES::SourceLogicalOperatorNodePtr &sourceOp) {
                               EndpointConfiguration source;
                               auto sourceNode = findNodeByOperator(sourceOp, gep).value();
                               source.schema = sourceOp->getOutputSchema();
                               source.subQueryID = sourceNode->getQuerySubPlans(1)[0]->getQuerySubPlanId();
                               source.nodeId = sourceNode->getTopologyNode()->getId();
                               source.operatorId = std::nullopt;
                               source.ip = sourceNode->getTopologyNode()->getIpAddress();
                               source.port = sourceNode->getTopologyNode()->getDataPort();
                               source.originId = sourceOp->getOriginId();
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

WorkerTCPSourceConfiguration buildTCPSource(const NES::NoOpSourceDescriptor &desc) {
    return WorkerTCPSourceConfiguration{desc.getTcp()->ip,
                                        desc.getTcp()->port,
                                        desc.getOriginId(),
                                        desc.getSchema()
    };
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

WorkerSourceConfiguration buildSource(const NES::SourceDescriptorPtr &sourceDescriptor) {
    if (sourceDescriptor->instanceOf<NES::Network::NetworkSourceDescriptor>()) {
        auto networkSource = sourceDescriptor->as<NES::Network::NetworkSourceDescriptor>();
        return WorkerSourceConfiguration{
                networkSource->getNesPartition().getOperatorId(),
                buildWorkerLink(networkSource),
                std::nullopt,
        };
    } else if (sourceDescriptor->instanceOf<NES::NoOpSourceDescriptor>()) {
        auto noOpSource = sourceDescriptor->as<NES::NoOpSourceDescriptor>();
        NES_ASSERT(noOpSource->getTcp().has_value(), "TCP Configuration is missing in Source Descriptor");
        return WorkerSourceConfiguration{
                noOpSource->getOperatorId(),
                std::nullopt,
                buildTCPSource(*noOpSource)
        };
    }

    NES_NOT_IMPLEMENTED();
}

WorkerStageConfiguration buildTreeRec(WorkerSubQueryStage current,
                                      const std::map<NES::PipelineId, WorkerSubQueryStage> &stages,
                                      const std::map<NES::PipelineId, NES::SourceDescriptorPtr> &sourceMap) {
    WorkerStageConfiguration config;
    config.stageId = current.stageId;
    config.numberOfOperatorHandlers = current.numberOfHandlers;

    auto filter = std::ranges::views::filter([&stages](auto pipelineId) {
        return stages.contains(pipelineId);
    });

    std::vector<WorkerStageConfiguration> predecessor;
    std::ranges::transform(current.predecessors | filter,
                           std::back_inserter(predecessor),
                           [&stages, &sourceMap](NES::PipelineId pipelineId) -> WorkerStageConfiguration {
                               return buildTreeRec(stages.at(pipelineId), stages, sourceMap);
                           });

    if (!predecessor.empty()) {
        config.predecessor.emplace(predecessor);
    }

    for (const auto &pred: current.predecessors) {
        if (sourceMap.contains(pred)) {
            config.upstream = buildSource(sourceMap.find(pred)->second);
        }
    }
    return config;
}

WorkerStageConfiguration buildTree(const SinkStage &sink,
                                   const std::vector<WorkerSubQueryStage> &workerSubQueryStage,
                                   const std::map<NES::PipelineId, NES::SourceDescriptorPtr> &sourceMap) {
    std::map<NES::PipelineId, WorkerSubQueryStage> stages;
    for (const auto &stage: workerSubQueryStage) {
        stages[stage.stageId] = stage;
    }
    return buildTreeRec(stages[sink.predecessor[0]], stages, sourceMap);
}

void YamlExport::addWorker(const std::vector<WorkerSubQuery> &subQueries, const NES::ExecutionNodePtr &workerNode) {
    std::vector<WorkerSubQueryConfiguration> subQueryConfiguration;
    std::ranges::transform(subQueries,
                           std::back_inserter(subQueryConfiguration),
                           [this](const WorkerSubQuery &subQuery) {
                               auto sink = subQuery.subplan->getOperatorByType<NES::SinkLogicalOperatorNode>();
                               NES_ASSERT2_FMT(sink.size() == 1, "Expected Single Sink: {}",
                                               subQuery.subplan->toString());
                               auto networkSinkDescriptor = sink[0]->getSinkDescriptor()->as<
                                       NES::Network::NetworkSinkDescriptor>();
                               auto type = WorkerDownStreamLinkConfigurationType::node;
                               std::optional<KafkaSinkConfiguration> kafkaSinkConfig = std::nullopt;
                               std::optional<WorkerLinkConfiguration> workerLink = std::nullopt;
                               if (exportToKafka.has_value() && networkSinkDescriptor->getNodeLocation().getNodeId() ==
                                                                SINK_NODE) {
                                   type = WorkerDownStreamLinkConfigurationType::kafka;
                                   kafkaSinkConfig.emplace(
                                           KafkaSinkConfiguration{sink[0]->getOutputSchema(), exportToKafka->broker,
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

                               NES_ASSERT2_FMT(subQuery.sinks.size() == 1, "Expected exactly one Sink");
                               NES_ASSERT2_FMT(subQuery.sinks.begin()->second.predecessor.size() == 1,
                                               "Sink Pipeline to have a single predecessor");
                               auto rootPipeline = subQuery.sinks.begin()->second.predecessor[0];
                               if (subQuery.sources.count(rootPipeline)) {
                                   return WorkerSubQueryConfiguration{
                                           std::nullopt,
                                           buildSource(subQuery.sources.find(rootPipeline)->second),
                                           subQuery.subplan->getQuerySubPlanId(),
                                           sink[0]->getOutputSchema()->getSchemaSizeInBytes(),
                                           type,
                                           workerLink,
                                           kafkaSinkConfig
                                   };
                               }

                               return WorkerSubQueryConfiguration{
                                       buildTree(subQuery.sinks.begin()->second, subQuery.stages, subQuery.sources),
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

YamlExport::YamlExport(const std::optional<ExportKafkaConfiguration> &exportToKafka)
        : exportToKafka(exportToKafka) {
}
