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

#ifndef NES_CORE_INCLUDE_SINKS_DATASINKPLUGIN_HPP_
#define NES_CORE_INCLUDE_SINKS_DATASINKPLUGIN_HPP_
#include <Util/PluginRegistry.hpp>
#include <optional>
namespace NES {

class SinkMedium;
using DataSinkPtr = std::shared_ptr<SinkMedium>;

/**
 * @brief DataSink interface to register a new physical data sink plugin.
 * Implementations of this plugin must implement createDataSink that translates a sink descriptor to a DataSinkPtr.
 */
class DataSinkPlugin {
  public:
    DataSinkPlugin() = default;
    /**
     * @brief Translates a sink descriptor to a data sink.
     * @param sinkId id of the sink
     * @param sinkDescriptor sink descriptor for which we want to create a physical data sink
     * @param schema of the result data
     * @param nodeEngine reference to the node engine
     * @param querySubPlan reference to the pipelined query plan
     * @param numOfProducers number of data producers.
     * @return
     */
    virtual std::optional<DataSinkPtr> createDataSink(OperatorId sinkId,
                                                      SinkDescriptorPtr sinkDescriptor,
                                                      SchemaPtr schema,
                                                      Runtime::NodeEnginePtr nodeEngine,
                                                      const QueryCompilation::PipelineQueryPlanPtr& querySubPlan,
                                                      size_t numOfProducers) = 0;
    virtual ~DataSinkPlugin() = default;
};

using SinkPluginRegistry = Util::PluginRegistry<DataSinkPlugin>;
}// namespace NES

#endif// NES_CORE_INCLUDE_SINKS_DATASINKPLUGIN_HPP_
