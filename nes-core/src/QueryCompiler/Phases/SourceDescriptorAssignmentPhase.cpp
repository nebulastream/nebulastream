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
#include <Catalogs/Source/PhysicalSourceTypes/BenchmarkSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MemorySourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/SenseSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/StaticDataSourceType.hpp>
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
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Exceptions/QueryCompilationException.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/SourceDescriptorAssignmentPhase.hpp>

namespace NES {
namespace QueryCompilation {

std::shared_ptr<SourceDescriptorAssignmentPhase> SourceDescriptorAssignmentPhase::create() {
    return std::make_shared<SourceDescriptorAssignmentPhase>();
}

PipelineQueryPlanPtr SourceDescriptorAssignmentPhase::apply(const PipelineQueryPlanPtr& queryPlan,
                                                            const std::vector<PhysicalSourcePtr>& physicalSources) {

    auto sourcePipelines = queryPlan->getSourcePipelines();
    for (const auto& pipeline : sourcePipelines) {
        //Convert logical source descriptor to actual source descriptor
        auto rootOperator = pipeline->getQueryPlan()->getRootOperators()[0];
        auto sourceOperator = rootOperator->as<PhysicalOperators::PhysicalSourceOperator>();
        auto sourceDescriptor = sourceOperator->getSourceDescriptor();
        if (sourceDescriptor->instanceOf<LogicalSourceDescriptor>()) {
            //Fetch logical and physical source name in the descriptor
            auto logicalSourceName = sourceDescriptor->getLogicalSourceName();
            auto physicalSourceName = sourceDescriptor->getPhysicalSourceName();
            //Iterate over all available physical sources
            for (const auto& physicalSource : physicalSources) {
                //Check if logical and physical source name matches with any of the physical source provided by the node
                if (physicalSource->getLogicalSourceName() == logicalSourceName
                    && physicalSource->getPhysicalSourceName() == physicalSourceName) {
                    auto newSourceDescriptor =
                        createSourceDescriptor(sourceOperator->getSourceDescriptor()->getSchema(), physicalSource);
                    sourceOperator->setSourceDescriptor(newSourceDescriptor);
                    break;
                }
            }
        }
    }
    return queryPlan;
}

SourceDescriptorPtr SourceDescriptorAssignmentPhase::createSourceDescriptor(SchemaPtr schema, PhysicalSourcePtr physicalSource) {
    auto logicalSourceName = physicalSource->getLogicalSourceName();
    auto physicalSourceName = physicalSource->getPhysicalSourceName();
    auto physicalSourceType = physicalSource->getPhysicalSourceType();
    auto sourceType = physicalSourceType->getSourceType();
    NES_DEBUG("PhysicalSourceConfig: create Actual source descriptor with physical source: {} {} ",
              physicalSource->toString(),
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
            for (const auto& plugin : SourceDescriptorPluginRegistry ::getPlugins()) {
                auto descriptor = plugin->create(schema, physicalSource);
                if (descriptor != nullptr) {
                    return descriptor;
                }
            }
            throw QueryCompilationException("PhysicalSourceConfig:: source type " + physicalSourceType->getSourceTypeAsString()
                                            + " not supported");
        }
    }
}

}// namespace QueryCompilation
}// namespace NES
