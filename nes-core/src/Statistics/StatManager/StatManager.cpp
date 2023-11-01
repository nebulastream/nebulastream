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

#include <limits>

#include "Statistics/StatManager/StatManager.hpp"
#include "Statistics/Requests/StatDeleteRequest.hpp"
#include "Statistics/Requests/StatProbeRequest.hpp"
#include "Statistics/StatManager/StatCollectorIdentifier.hpp"

namespace NES {

namespace Experimental::Statistics {

double StatManager::probeStat(StatCollectorIdentifier& statCollectorIdentifier) {
    statCollectorIdentifier.getPhysicalSourceName();
    return 1.0;
}

void StatManager::probeStats(StatProbeRequest& probeRequest, ProbeStatReply* stats) {

    probeRequest.getPhysicalSourceNames();

    auto allPhysicalSourceNames = probeRequest.getPhysicalSourceNames();
    auto statCollectorIdentifier =
        StatCollectorIdentifier(allPhysicalSourceNames.at(0), probeRequest.getFieldName(), probeRequest.getStatCollectorType());

    double stat;
    for (uint64_t index = 0; index < probeRequest.getPhysicalSourceNames().size(); index++) {
        statCollectorIdentifier.setPhysicalSourceName(allPhysicalSourceNames.at(0));
        stat = probeStat(statCollectorIdentifier);
        // ToDo: add try catch block
        if (stat != std::numeric_limits<double>::quiet_NaN()) {
            stats->add_stats(stat);
        } else {
            stats->Clear();
            return;
        }
    }
    return;
}

int64_t StatManager::deleteStat(StatDeleteRequest& deleteRequestParamObj) {
    deleteRequestParamObj.getLogicalSourceName();
    return 1;
}

}// namespace Experimental::Statistics
}// namespace NES