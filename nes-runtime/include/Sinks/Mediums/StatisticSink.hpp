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

#ifndef NES_NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_STATISTICSINK_HPP_
#define NES_NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_STATISTICSINK_HPP_

#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/StatisticCollectorType.hpp>

namespace NES::Experimental::Statistics {

class StatisticCollectorStorage;
using StatisticCollectorStoragePtr = std::shared_ptr<StatisticCollectorStorage>;

class StatisticCollectorFormat;
using StatisticCollectorFormatPtr = std::shared_ptr<StatisticCollectorFormat>;

/**
 * @brief a sink that writes statisticCollectors to the StatisticCollectorStorage
 */
class StatisticSink : public SinkMedium {
  public:
    /**
     * @brief the constructor for a StatisticSink that writes statisticCollector objects to statisticCollectorStorages
     * @param statisticCollectorStorage the storage to which the statisticCollectors are to be written to
     * @param statisticCollectorType the type of statistics that are going to be written by the sink
     * @param sinkFormat in which the data is written
     * @param nodeEngine the nodeEngine
     * @param numOfProducers
     * @param queryId the query producing the tuple(Buffers)
     * @param querySubPlanId
     * @param numberOfOrigins number of origins of a given query
     */
    StatisticSink(StatisticCollectorStoragePtr statisticCollectorStorage,
                  StatisticCollectorType statisticCollectorType,
                  SinkFormatPtr sinkFormat,
                  Runtime::NodeEnginePtr const& nodeEngine,
                  uint32_t numOfProducers,
                  QueryId queryId,
                  QuerySubPlanId querySubPlanId,
                  uint64_t numberOfOrigins);

    virtual ~StatisticSink() = default;

    /**
     * @brief only required because of inheritance
     */
    void setup() override;

    /**
     * @brief only required because of inheritance
     */
    void shutdown() override;

    /**
     * @brief not yet implemented by any statisticCollectors as it is not yet needed
     * @param inputBuffer
     * @return
     */
    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) override;

    /**
     * @return returns the statisticCollectorType as a string, specifically for the toString() method
     */
    std::string getStatisticCollectorTypeAsString() const;

    /**
     * @return returns a compact description of the object as a string
     */
    std::string toString() const override;

    /**
     * @return returns the type of the string (STATISTIC_COLLECTOR_STORAGE_SINK)
     */
    SinkMediumTypes getSinkMediumType() override;

  private:
    StatisticCollectorStoragePtr statisticCollectorStorage;
    StatisticCollectorType statisticCollectorType;
    StatisticCollectorFormatPtr statisticCollectorFormat;
};
}// namespace NES::Experimental::Statistics
#endif//NES_NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_STATISTICSINK_HPP_
