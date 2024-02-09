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

#include <Statistics/CountMin.hpp>
#include <cmath>
#include <vector>

namespace NES::Experimental::Statistics {

CountMin::CountMin(uint64_t width,
                   const std::vector<uint64_t>& data,
                   StatisticCollectorIdentifierPtr statisticCollectorIdentifier,
                   const uint64_t observedTuples,
                   const uint64_t depth,
                   const uint64_t startTime,
                   const uint64_t endTime)
    : Statistic(std::move(statisticCollectorIdentifier), observedTuples, depth, startTime, endTime), width(width),
      error(calcError(width)), probability(calcProbability(depth)), data(data) {}

double CountMin::calcError(const uint64_t width) { return 1.0 / (double) width; }

double CountMin::calcProbability(const uint64_t depth) { return exp(-1.0 * (double) depth); }

const std::vector<uint64_t>& CountMin::getData() const { return data; }

uint64_t CountMin::getWidth() const { return width; }

double CountMin::getError() const { return error; }

double CountMin::getProbability() const { return probability; }

CountMin CountMin::createFromString(void* cmString, uint64_t depth, uint64_t width) {
    auto* rawData = static_cast<uint64_t*>(cmString);
    std::vector<uint64_t> cmData(rawData, rawData + depth * width);

    return {width, cmData, nullptr, 0, depth, 0, 0};
}

void CountMin::incrementCounter(uint64_t* data, uint64_t rowId, uint64_t width, uint64_t columnId) {
    data[rowId * width + columnId] += 1;
}
}// namespace NES::Experimental::Statistics
