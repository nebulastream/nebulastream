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

#ifndef NES_INCLUDE_SOURCES_MONITORINGSOURCE_HPP_
#define NES_INCLUDE_SOURCES_MONITORINGSOURCE_HPP_

#include <Monitoring/MetricCollectors/MetricCollectorType.hpp>
#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Sources/DataSource.hpp>
#include <chrono>

namespace NES {

class MonitoringSource : public DataSource {
  public:
    MonitoringSource(MetricCollectorPtr metricCollector,
                     std::chrono::milliseconds waitTime,
                     Runtime::BufferManagerPtr bufferManager,
                     Runtime::QueryManagerPtr queryManager,
                     OperatorId operatorId,
                     OriginId originId,
                     size_t numSourceLocalBuffers,
                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors = {});

    SourceType getType() const override;

    std::optional<Runtime::TupleBuffer> receiveData() override;

    MetricCollectorType getCollectorType();
    std::chrono::milliseconds getWaitTime() const;

    /**
     * @brief override the toString method for the binary source
     * @return returns string describing the binary source
     */
    std::string toString() const override;

  private:
    MetricCollectorPtr metricCollector;
    std::chrono::milliseconds waitTime;
};

using MonitoringSourcePtr = std::shared_ptr<MonitoringSource>;
};// namespace NES

#endif// NES_INCLUDE_SOURCES_MONITORINGSOURCE_HPP_
