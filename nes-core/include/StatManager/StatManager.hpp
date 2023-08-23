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

    public:
      ~StatManager() = default;

      /**
       * @brief returns a reference to unique pointer of a vector, which contains unique pointers to statCollectors
       * @return statCollectors
       */
      std::vector<StatCollectorPtr>& getStatCollectors();

      /**
       * @brief starts a statistic query from which statCollectors are generated
       * @param config a configuration object specifying parameters such as the logicalSourceName, physicalSourceName, schema and more
       * @param nodeEngine a nodeEngine of the coordinator or of a worker
       * @return queryId of the launched query
       */
      uint64_t createStat(const StatCollectorConfig& config, const Runtime::NodeEnginePtr nodeEngine);

      /**
       * @brief queries a specific statistic
       * @param config a configuration object specifying parameters such as the logicalSourceName, physicalSourceName, schema and more,
       * such that the specific statCollector can be identified
       * @param key a key to query the specific statistic for
       * @return a double value for the specific statistic
       */
      double queryStat(const StatCollectorConfig& config,
                       const uint32_t key);

      /**
       * @brief
       * @param config a configuration object specifying parameters such as the logicalSourceName, physicalSourceName, schema and more,
       * such that the specific statCollector, which is to be deleted, can be identified
       * @param nodeEngine a nodeEngine of the coordinator or of a worker
       * @return a boolean describing wether the operation was successful or not
       */
      bool deleteStat(const StatCollectorConfig& config, const Runtime::NodeEnginePtr nodeEngine);

    private:
      std::vector<StatCollectorPtr> statCollectors {};
      Optimizer::TypeInferencePhasePtr typeInferencePhase;
  };

} // Experimental::Statistics
} // NES

#endif //NES_CORE_INCLUDE_STATMANAGER_STATMANAGER_HPP
