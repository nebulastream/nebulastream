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
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Statistic.hpp>

namespace NES::Statistic {

Statistic::~Statistic() = default;

Windowing::TimeMeasure Statistic::getStartTs() const { return startTs; }

Windowing::TimeMeasure Statistic::getEndTs() const { return endTs; }

uint64_t Statistic::getObservedTuples() const { return observedTuples; }

bool Statistic::operator==(const Statistic& other) const { return this->equal(other); }

Statistic::Statistic(const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs, uint64_t observedTuples)
    : startTs(startTs), endTs(endTs), observedTuples(observedTuples) {}
} // namespace NES::Statistic