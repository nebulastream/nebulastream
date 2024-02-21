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

#include <Statistics/Statistic.hpp>
#include <Util/StatisticCollectorIdentifier.hpp>

namespace NES::Experimental::Statistics {

Statistic::Statistic(StatisticCollectorIdentifierPtr statisticCollectorIdentifier,
                     const uint64_t observedTuples,
                     const uint64_t depth)
    : statisticCollectorIdentifier(statisticCollectorIdentifier), observedTuples(observedTuples), depth(depth) {}

StatisticCollectorIdentifierPtr Statistic::getStatisticCollectorIdentifier() const { return statisticCollectorIdentifier; }

uint64_t Statistic::getObservedTuples() const { return observedTuples; }

uint64_t Statistic::getDepth() const { return depth; }

uint64_t Statistic::getStartTime() const { return statisticCollectorIdentifier->getStartTime(); }

uint64_t Statistic::getEndTime() const { return statisticCollectorIdentifier->getEndTime(); }

}// namespace NES::Experimental::Statistics
