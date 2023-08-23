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

#include <Util/Logger/Logger.hpp>
#include <StatManager/StatManager.hpp>
#include <StatManager/StatCollectors/StatCollectorConfiguration.hpp>
#include <StatManager/StatCollectors/Synopses/Sketches/CountMin/CountMin.hpp>
#include <StatManager/StatCollectors/StatCollector.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/Execution/ExecutableQueryPlanStatus.hpp>
#include <API/QueryAPI.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>

namespace NES::Experimental::Statistics {

	std::vector<StatCollectorPtr>& StatManager::getStatCollectors() {
		return statCollectors;
	}

  uint64_t StatManager::createStat (const StatCollectorConfig& config, const Runtime::NodeEnginePtr nodeEngine) {
    auto physicalSourceName = config.getPhysicalSourceName();
    auto field = config.getField();
    auto duration = config.getDuration();
    auto frequency = config.getFrequency();

    bool found = false;

    // check if the stat is already being tracked passively
    for (const auto &collector: statCollectors) {
      // could add a hashmap or something here that first checks if a statCollector can even generate the according statistic
      if (collector->getPhysicalSourceName() == physicalSourceName && collector->getField() == field &&
          collector->getDuration() == duration && collector->getFrequency() == frequency) {
        found = true;
        break;
      }
    }

    auto statMethodName = config.getStatMethodName();

    // if the stat is not yet tracked, then create it
    if (!found) {
      auto logicalStreamName = config.getLogicalSourceName();
      auto schema = config.getSchema();

      if (statMethodName.compare("FrequencyCM")) {
        // get config values
        auto error = static_cast<double>(config.getError());
        auto probability = static_cast<double>(config.getProbability());
        auto depth = static_cast<uint32_t>(config.getDepth());
        auto width = static_cast<uint32_t>(config.getWidth());

        // check that only error and probability or depth and width are set
        if (((depth != 0 && width != 0) && (error == 0.0 && probability == 0.0)) ||
            ((depth == 0 && width == 0) && (error != 0.0 && probability != 0.0))) {
          // check if error and probability are set and within valid value ranges and if so, create a CM Sketch
          if (error != 0.0 && probability != 0.0) {
            if (error <= 0 || error >= 1) {
              NES_DEBUG("Sketch error hat invalid value. Must be greater than zero, but smaller than 1.");

            } else if (probability <= 0 || probability >= 1) {
              NES_DEBUG("Sketch error hat invalid value. Must be greater than zero, but smaller than 1.");

            } else {
              // create stat
              std::unique_ptr<CountMin> cm = std::make_unique<CountMin>(config);

              statCollectors.push_back(std::move(cm));

              Catalogs::UDF::UDFCatalogPtr udfCatalog = Catalogs::UDF::UDFCatalog::create();
              // We inject an invalid query parsing service as it is not used in the tests.
              auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
              sourceCatalog->addLogicalSource(logicalStreamName, schema);
              typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

              auto originIdInferencePhase = Optimizer::OriginIdInferencePhase::create();

              auto query = Query::from(logicalStreamName).filter(Attribute(field) <= 2).sink(PrintSinkDescriptor::create());
              auto queryPlan = query.getQueryPlan();
              auto queryId = NES::PlanIdGenerator::getNextQueryId();
              queryPlan->setQueryId(queryId);

              queryPlan = typeInferencePhase->execute(queryPlan);
              queryPlan = originIdInferencePhase->execute(queryPlan);

              auto srcOperators = queryPlan->getSourceOperators();
              srcOperators[0]->getSourceDescriptor()->setPhysicalSourceName(physicalSourceName);
              srcOperators[0]->getSourceDescriptor()->setSchema(schema);
              bool success = nodeEngine->registerQueryInNodeEngine(query.getQueryPlan());

              success = nodeEngine->startQuery(queryPlan->getQueryId());

              success = nodeEngine->getQueryStatus(queryId) == Runtime::Execution::ExecutableQueryPlanStatus::Running;

              return queryId;
            }
          }
        }
      // TODO: Potentially add more Synopses here dependent on Nautilus integration.
      } else {
        NES_DEBUG("statMethodName is not defined!");
        return false;
      }
    }
    NES_DEBUG("Statistic already exists!")
    return true;
  }

//  bool StatManager::createStat (const StatCollectorConfig& config) {
//    auto physicalSourceName = config.getPhysicalSourceName();
//    auto field = config.getField();
//    auto duration = config.getDuration();
//    auto frequency = config.getFrequency();
//
//    bool found = false;
//
//    // check if the stat is already being tracked passively
//    for (const auto &collector: statCollectors) {
//      // could add a hashmap or something here that first checks if a statCollector can even generate the according statistic
//      if (collector->getPhysicalSourceName() == physicalSourceName && collector->getField() == field &&
//          collector->getDuration() == duration && collector->getFrequency() == frequency) {
//        found = true;
//        break;
//      }
//    }
//
//    auto statMethodName = config.getStatMethodName();
//
//    // if the stat is not yet tracked, then create it
//    if (!found) {
//      if (statMethodName.compare("FrequencyCM")) {
//        // get config values
//        auto error = static_cast<double>(config.getError());
//        auto probability = static_cast<double>(config.getProbability());
//        auto depth = static_cast<uint32_t>(config.getDepth());
//        auto width = static_cast<uint32_t>(config.getWidth());
//
//        // check that only error and probability or depth and width are set
//        if (((depth != 0 && width != 0) && (error == 0.0 && probability == 0.0)) ||
//            ((depth == 0 && width == 0) && (error != 0.0 && probability != 0.0))) {
//          // check if error and probability are set and within valid value ranges and if so, create a CM Sketch
//          if (error != 0.0 && probability != 0.0) {
//            if (error <= 0 || error >= 1) {
//              NES_DEBUG("Sketch error hat invalid value. Must be greater than zero, but smaller than 1.");
//
//            } else if (probability <= 0 || probability >= 1) {
//              NES_DEBUG("Sketch error hat invalid value. Must be greater than zero, but smaller than 1.");
//
//            } else {
//              // create stat
//              std::unique_ptr<CountMin> cm = std::make_unique<CountMin>(config);
//
//              statCollectors.push_back(std::move(cm));
//
//              return true;
//            }
//          }
//        }
//        // TODO: Potentially add more Synopses here dependent on Nautilus integration.
//      } else {
//        NES_DEBUG("statMethodName is not defined!");
//        return false;
//      }
//    }
//    NES_DEBUG("Stat already exists!")
//    return true;
//  }

  double StatManager::queryStat (const StatCollectorConfig& config,
                                 const uint32_t key) {
    auto physicalSourceName = config.getPhysicalSourceName();
    auto field = config.getField();
    auto duration = static_cast<time_t>(config.getDuration());
    auto frequency = static_cast<time_t>(config.getFrequency());

    // check if the stat is already being tracked passively
    for (const auto& collector : statCollectors) {
      // could add a hashmap or something here that first checks if a statCollector can even generate the according statistic
      if (collector->getPhysicalSourceName() == physicalSourceName && collector->getField() == field &&
          collector->getDuration() == duration && collector->getFrequency() == frequency) {
        auto statMethodName = config.getStatMethodName();

        if (statMethodName.compare("FrequencyCM")) {
          auto cm = dynamic_cast<CountMin*>(collector.get());
          return cm->pointQuery(key);
        } else {
          NES_DEBUG("No compatible combination of statistic and method for querying of statCollector!");
        }
      }
    }
    return -1.0;
  }

  bool StatManager::deleteStat(const StatCollectorConfig& config, const Runtime::NodeEnginePtr nodeEngine) {
    auto physicalSourceName = config.getPhysicalSourceName();
    auto field = config.getField();
    auto duration = config.getDuration();
    auto frequency = config.getFrequency();
    auto statMethodName = config.getStatMethodName();

    for (auto it = statCollectors.begin(); it != statCollectors.end(); it++) {
      auto& collector = *it;

      if (collector->getPhysicalSourceName() == physicalSourceName && collector->getField() == field &&
          collector->getDuration() == duration && collector->getFrequency() == frequency) {

        auto queryId = config.getQueryId();
        if (statMethodName.compare("FrequencyCM")) {
          statCollectors.erase(it);

          bool success = nodeEngine->stopQuery(queryId);

          if (!success) {
            NES_DEBUG("Stopping query was successful {}", success)
          }

          success = nodeEngine->unregisterQuery(queryId);

          if (!success) {
            NES_DEBUG("Unregistering query was successful {}", success)
          }

          return success;

          } else {
            NES_DEBUG("Could not delete Statistic Object!");
            return false;
        }
      }
    }
    NES_DEBUG("Could not delete Statistic Object!");
    return false;
  }

} // NES::Experimental::Statistics
