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

#ifndef NES_CORE_INCLUDE_STATMANAGER_STATMANAGER_HPP
#define NES_CORE_INCLUDE_STATMANAGER_STATMANAGER_HPP

#include <string>
#include <vector>
#include <memory>
#include <math.h>

namespace NES {

namespace Runtime {
  class NodeEngine;
  using NodeEnginePtr = std::shared_ptr<NodeEngine>;
}

namespace Optimizer {

  class TypeInferencePhase;
  using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;
}

namespace Experimental::Statistics {

  class StatCollectorConfig;

  class StatCollector;
  using StatCollectorPtr = std::unique_ptr<StatCollector>;

  class StatManager {
    private:
      std::vector<StatCollectorPtr> statCollectors {};
      Optimizer::TypeInferencePhasePtr typeInferencePhase;

    public:
      ~StatManager() = default;

      std::vector<StatCollectorPtr>& getStatCollectors();

      bool createStat(const StatCollectorConfig& config);

      bool createStat(const StatCollectorConfig& config, const Runtime::NodeEnginePtr nodeEngine);

      double queryStat(const StatCollectorConfig& config,
                       const uint32_t key);

      void deleteStat(const StatCollectorConfig& config);
  };

} // Experimental::Statistics
} // NES
#endif //NES_CORE_INCLUDE_STATMANAGER_STATMANAGER_HPP
