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

#include <StatisticCollection/StatisticKey.hpp>
#include <Util/Logger/Logger.hpp>
#include <sstream>
#include <utility>

namespace NES::Statistic {

StatisticKey::StatisticKey(MetricPtr metric, StatisticId statisticId) : metric(std::move(metric)), statisticId(statisticId) {}

bool StatisticKey::operator==(const StatisticKey& rhs) const {
    return (*metric) == (*rhs.metric) && statisticId == rhs.statisticId;
}

bool StatisticKey::operator!=(const StatisticKey& rhs) const { return !(rhs == *this); }

std::string StatisticKey::toString() const {
    std::ostringstream oss;
    oss << "Metric(" << metric->toString() << ") ";
    oss << "StatisticId(" << statisticId << ")";
    return oss.str();
}

StatisticHash StatisticKey::combineStatisticIdWithMetricHash(StatisticMetricHash metricHash, StatisticId statisticId) {
    return statisticId ^ (metricHash + goldenRatio);
}

StatisticHash StatisticKey::hash() const { return combineStatisticIdWithMetricHash(metric->hash(), statisticId); }

}// namespace NES::Statistic