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

#ifndef NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_STATISTICSINK_HPP_
#define NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_STATISTICSINK_HPP_

#include <Sinks/Mediums/SinkMedium.hpp>

namespace NES::Statistic {

class AbstractStatisticStore;
using StatisticStorePtr = std::shared_ptr<AbstractStatisticStore>;

class AbstractStatisticFormat;
using StatisticFormatPtr = std::shared_ptr<AbstractStatisticFormat>;

/**
 * @brief Physical sink that receives a tuple buffer, extracts statistics, and writes these statistics
 * to the statisticStore
 */
class StatisticSink : public SinkMedium {
  public:
    StatisticSink(const SinkFormatPtr& sinkFormat,
                  const Runtime::NodeEnginePtr& nodeEngine,
                  uint32_t numOfProducers,
                  SharedQueryId sharedQueryId,
                  DecomposedQueryId decomposedQueryId,
                  DecomposedQueryPlanVersion decomposedQueryVersion,
                  FaultToleranceType faultToleranceType,
                  uint64_t numberOfOrigins,
                  StatisticStorePtr statisticStore,
                  StatisticFormatPtr statisticSinkFormat);

    void setup() override;
    void shutdown() override;
    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) override;
    std::string toString() const override;
    SinkMediumTypes getSinkMediumType() override;

  private:
    StatisticStorePtr statisticStore;
    StatisticFormatPtr statisticSinkFormat;
};

}// namespace NES::Statistic

#endif// NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_STATISTICSINK_HPP_
