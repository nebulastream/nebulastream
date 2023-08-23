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

#ifndef NES_CORE_INCLUDE_CONFIGURATIONS_STATMANAGERCONFIGURATION_HPP
#define NES_CORE_INCLUDE_CONFIGURATIONS_STATMANAGERCONFIGURATION_HPP

#include <math.h>
#include <ctime>
#include <string>
#include <memory>

namespace NES {

  using QueryId = uint64_t;

  class Schema;
  using SchemaPtr = std::shared_ptr<Schema>;

  namespace Experimental::Statistics {

    /**
     * @brief a general purpose configuration object for statCollectors
     */
    class StatCollectorConfig {

    public:
      /**
       * @brief returns the logicalSourceName over which to construct the statCollector
       * @return physicalSourceName
       */
      const std::string &getLogicalSourceName() const {
        return logicalSourceName;
      }

      /**
       * @brief returns the physicalSourceName over which to construct the statCollector
       * @return physicalSourceName
       */
      const std::string &getPhysicalSourceName() const {
        return physicalSourceName;
      }

      /**
       * @brief returns the schema over which to construct the statCollector
       * @return schema
       */
      SchemaPtr getSchema() const {
        return schema;
      }

      /**
       * @brief sets the schema of the config
       * @param newSchema the schema of the data stream on which the statCollector is to be constructed on
       */
      void setSchema(const SchemaPtr& newSchema) {
        schema = newSchema;
      }

      /**
       * @brief returns the queryId of the query which is launched or has been launched to generate the statCollector
       * @return queryId
       */
      QueryId getQueryId() const {
        return queryId;
      }

      /**
       * @brief sets the value of the queryId, which is being used or will be used to generate a statCollector
       * @param newQueryId a valid queryId
       */
      void setQueryId(QueryId newQueryId) {
        queryId = newQueryId;
      }

      /**
       * @brief returns the field over which to construct the statCollector
       * @return field
       */
      const std::string &getField() const {
        return field;
      }

      /**
       * @brief returns the statCollector type to use
       * @return statMethodName
       */
      const std::string &getStatMethodName() const {
        return statMethodName;
      }

      /**
       * @brief returns the duration for which the statCollector generates one or more statistics
       * @return duration
       */
      time_t getDuration() const {
        return duration;
      }

      /**
       * @brief gets the frequency in which the statistic(s) is/are updated
       * @return frequency
       */
      time_t getFrequency() const {
        return frequency;
      }

      /**
       * @brief gets the error of the statCollector
       * @return error
       */
      double_t getError() const {
        return error;
      }

      /**
       * @brief gets the probability of the error exceeding the parametrized error
       * @return probability
       */
      double_t getProbability() const {
        return probability;
      }

      /**
       * @brief gets the depth of the statCollector
       * @return depth
       */
      uint32_t getDepth() const {
        return depth;
      }

      /**
       * @brief gets the width of the statCollector
       * @return width
       */
      uint32_t getWidth() const {
        return width;
      }

      /**
       * @brief the constructor of the statCollectorConfig, which defines which type of a statCollector to construct and
       * with which parametrization
       * @param logicalSourceName the logicalSourceName
       * @param physicalSourceName the physicalSourceName over which to construct the statCollector
       * @param fieldName the fieldName over which to construct the statCollector
       * @param methodName the statCollector type to construct the statistic(s)
       * @param dur the duration for which the statCollector generates the statistic(s)
       * @param freq the frequency in which the statCollector updates the statistic(s)
       * @param err the potential error in the statistic
       * @param prob the probability that the actual error of one approximation exceeds the parametrized error
       * @param dep the depth of a statCollector
       * @param wid the width of a statCollector
       */
      StatCollectorConfig(const std::string &logicalSourceName = "defaultLogicalSourceName",
                          const std::string &physicalSourceName = "defaultPhysicalSourceName",
                          const std::string &fieldName = "defaultFieldName",
                          const std::string &methodName = "defaultStatMethodName", uint32_t dur = 1,
                          uint32_t freq = 100,
                          double_t err = 0.0001, double_t prob = 0.0001, uint32_t dep = 0, uint32_t wid = 0)
          : logicalSourceName(logicalSourceName), physicalSourceName(physicalSourceName), field(fieldName),
            statMethodName(methodName), duration(dur), frequency(freq), error(err), probability(prob),
            depth(dep), width(wid) {
      }

    private:
      const std::string logicalSourceName;
      const std::string physicalSourceName;
      SchemaPtr schema;
      QueryId queryId;
      const std::string field;
      const std::string statMethodName;
      const time_t duration;
      const time_t frequency;
      const double_t error;
      const double_t probability;
      const uint32_t depth;
      const uint32_t width;
    };
  } // Experimental::Statistics
} // NES
#endif //NES_CORE_INCLUDE_CONFIGURATIONS_STATMANAGERCONFIGURATION_HPP
