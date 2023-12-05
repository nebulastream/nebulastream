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

#ifndef NES_NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_STATISTICCOLLECTORSTORAGESINK_HPP_
#define NES_NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_STATISTICCOLLECTORSTORAGESINK_HPP_

#include <Sinks/Mediums/SinkMedium.hpp>
#include "../nes-worker/include/Statistics/StatisticCollectors/StatisticCollectorType.hpp"

namespace NES::Experimental::Statistics {

class StatisticCollectorStorageSink : public SinkMedium {
  public:
    StatisticCollectorStorageSink(StatisticCollectorType statisticCollectorType,
                                  SinkFormatPtr sinkFormat,
                                  Runtime::NodeEnginePtr nodeEngine,
                                  uint32_t numOfProducers,
                                  QueryId queryId,
                                  QuerySubPlanId querySubPlanId,
                                  FaultToleranceType faultToleranceType,
                                  uint64_t numberOfOrigins);

    void setup() override;

    void shutdown() override;

    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) override;

    std::string getTypeAsString() const;

    std::string toString() const override;

    SinkMediumTypes getSinkMediumType() override;

  private:
    StatisticCollectorType statisticCollectorType;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_STATISTICCOLLECTORSTORAGESINK_HPP_
