/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_SOURCES_MONITORING_SOURCE_HPP_
#define NES_INCLUDE_SOURCES_MONITORING_SOURCE_HPP_

#include <Sources/DefaultSource.hpp>
#include <chrono>

namespace NES {

class MonitoringPlan;
using MonitoringPlanPtr = std::shared_ptr<MonitoringPlan>;

class MetricGroup;
using MetricGroupPtr = std::shared_ptr<MetricGroup>;

class MetricCatalog;
using MetricCatalogPtr = std::shared_ptr<MetricCatalog>;

class MonitoringSource : public DefaultSource {
  public:
    MonitoringSource(const MonitoringPlanPtr& monitoringPlan,
                     MetricCatalogPtr metricCatalog,
                     Runtime::BufferManagerPtr bufferManager,
                     Runtime::QueryManagerPtr queryManager,
                     uint64_t numbersOfBufferToProduce,
                     uint64_t frequency,
                     OperatorId operatorId,
                     size_t numSourceLocalBuffers,
                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors = {});

    SourceType getType() const override;

    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the binary source
     * @return returns string describing the binary source
     */
    std::string toString() const override;

  private:
    MonitoringPlanPtr monitoringPlan;
    MetricCatalogPtr metricCatalog;
    MetricGroupPtr metricGroup;
};

using MonitoringSourcePtr = std::shared_ptr<MonitoringSource>;
};// namespace NES

#endif// NES_INCLUDE_SOURCES_MONITORING_SOURCE_HPP_
