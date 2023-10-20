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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_REQUESTS_PROBEREQUESTPARAMOBJ_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_REQUESTS_PROBEREQUESTPARAMOBJ_HPP_

#include <string>
#include <memory>
#include <vector>

#include <Statistics/Requests/RequestParamObj.hpp>
#include <Statistics/StatCollectors/StatCollectorType.hpp>

namespace NES {

namespace Experimental::Statistics {

/**
 * @brief the inherited class that defines what is needed to probe Statistics
 */
class ProbeRequestParamObj : public RequestParamObj {
  public:
    ProbeRequestParamObj(const std::string &logicalSourceName,
                         const std::string &fieldName,
                         const StatCollectorType statCollectorType,
                         const std::string &probeExpression,
                         const std::vector<std::string> &physicalSourceNames,
                         const time_t startTime,
                         const time_t endTime,
                         const bool merge = false)
        : RequestParamObj(logicalSourceName, fieldName, statCollectorType),
          probeExpression(probeExpression), physicalSourceNames(physicalSourceNames),
          startTime(startTime), endTime(endTime), merge(merge) {}

    /**
     * @return returns the expression that is used to define what stat(s) are to be queried
     */
    std::string getProbeExpression() const {
        return probeExpression;
    }

    /**
     * @return returns the physicalSourceNames over which the statCollectors were generated that we wish to probe/query
     */
    std::vector<std::string> getPhysicalSourceNames() {
        return physicalSourceNames;
    }

    /**
     * @return returns the first possibleTime for which we want to query/probe one or more statistics
     */
    time_t getStartTime() const {
        return startTime;
    }

    /**
     * @return returns the last possible time for which we want to probe/query one or more statistics
     */
    time_t getEndTime() const {
        return endTime;
    }

    /**
     * @return returns true or false and describes wether we want to merge multiple local statistics or
     * output them seperatly
     */
    bool getMerge() const {
        return merge;
    }

  private:
    std::string probeExpression;
    std::vector<std::string> physicalSourceNames;
    time_t startTime;
    time_t endTime;
    bool merge;
};
}

}

#endif //NES_NES_CORE_INCLUDE_STATISTICS_REQUESTS_PROBEREQUESTPARAMOBJ_HPP_
