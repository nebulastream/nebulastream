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
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sinks/Arrow/ArrowFileSink.hpp>
#include <Sinks/Arrow/ArrowFormat.hpp>
#include <Sinks/DataSinkPlugin.hpp>

namespace NES {

class ArrowFileSinkPlugin : public DataSinkPlugin {
  public:
    ArrowFileSinkPlugin() { NES_INFO("ArrowFileSinkPlugin loaded"); }
    std::optional<DataSinkPtr> createDataSink(OperatorId,
                                              SinkDescriptorPtr sinkDescriptor,
                                              SchemaPtr schema,
                                              Runtime::NodeEnginePtr nodeEngine,
                                              const QueryCompilation::PipelineQueryPlanPtr& querySubPlan,
                                              size_t numOfProducers) override {
        if (sinkDescriptor->instanceOf<FileSinkDescriptor>()) {
            auto fileSinkDescriptor = sinkDescriptor->as<FileSinkDescriptor>();
            if (fileSinkDescriptor->getSinkFormatAsString() == "ARROW_FORMAT") {
                auto format = std::make_shared<ArrowFormat>(schema, nodeEngine->getBufferManager());
                return std::make_shared<ArrowFileSink>(format,
                                                       nodeEngine,
                                                       numOfProducers,
                                                       fileSinkDescriptor->getFileName(),
                                                       fileSinkDescriptor->getAppend(),
                                                       querySubPlan->getQueryId(),
                                                       querySubPlan->getQuerySubPlanId(),
                                                       fileSinkDescriptor->getFaultToleranceType(),
                                                       fileSinkDescriptor->getNumberOfOrigins());
            }
        }
        return {};
    }
    ~ArrowFileSinkPlugin() override { NES_INFO("ArrowFileSinkPlugin un-loaded"); }
};

// Register sink plugin
[[maybe_unused]] static SinkPluginRegistry::Add<ArrowFileSinkPlugin> arrowSinkPlugin;

}// namespace NES