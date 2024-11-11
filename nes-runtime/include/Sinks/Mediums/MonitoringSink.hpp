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

#ifndef NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_MONITORINGSINK_HPP_
#define NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_MONITORINGSINK_HPP_

#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/MetricCollectorType.hpp>
#include <cstdint>
#include <memory>
#include <string>

namespace NES {

namespace Monitoring {
class AbstractMetricStore;
using MetricStorePtr = std::shared_ptr<AbstractMetricStore>;
}// namespace Monitoring

class Metric;
using MetricPtr = std::shared_ptr<Metric>;

/**
 * @brief this class provides a monitoring sink to collecct metrics based on a given metric collector.
 */
class MonitoringSink : public SinkMedium {
  public:
    /**
     * @brief Default getSliceIndexByTs for could not find a slice,
     * @Note the default output will be written to cout
     */
    explicit MonitoringSink(SinkFormatPtr sinkFormat,
                            Monitoring::MetricStorePtr metricStore,
                            Monitoring::MetricCollectorType collectorType,
                            Runtime::NodeEnginePtr nodeEngine,
                            uint32_t numOfProducers,
                            SharedQueryId sharedQueryId,
                            DecomposedQueryId decomposedQueryId,
                            DecomposedQueryPlanVersion decomposedQueryVersion,
                            FaultToleranceType faultToleranceType = FaultToleranceType::NONE,
                            uint64_t numberOfOrigins = 1);

    /**
     * @brief destructor
     * @Note this is required by some tests
     */
    ~MonitoringSink() override;

    /**
     * @brief setup method for print sink
     * @Note required due to derivation but does nothing
     */
    void setup() override;

    /**
     * @brief shutdown method for print sink
     * @Note required due to derivation but does nothing
     */
    void shutdown() override;

    /**
     * @brief method to write the content of a tuple buffer to output console
     * @param tuple buffer to write
     * @return bool indicating success of the write
     */
    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) override;

    /**
     * @brief override the toString method for the print sink
     * @return returns string describing the print sink
     */
    std::string toString() const override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType() override;

  private:
    Monitoring::MetricStorePtr metricStore;
    Monitoring::MetricCollectorType collectorType;
};
using MonitoringSinkPtr = std::shared_ptr<MonitoringSink>;

}// namespace NES

#endif// NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_MONITORINGSINK_HPP_
