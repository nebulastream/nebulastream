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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_COLLECTORTRIGGER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_COLLECTORTRIGGER_HPP_

#include <Execution/StatisticsCollector/Statistic.hpp>

namespace NES::Runtime::Execution {

enum TriggerType { PipelineStatisticsTrigger };

/**
 * @brief triggers statistics collector
 */
class CollectorTrigger {
  public:
    explicit CollectorTrigger(TriggerType triggerType);

    TriggerType getTriggerType();

    std::string getTypeAsString();

  private:
    TriggerType triggerType;

};
}// namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_COLLECTORTRIGGER_HPP_