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

#include <vector>
#include <Statistics/ReservoirSample.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Experimental::Statistics {

ReservoirSample::ReservoirSample(const std::vector<uint64_t>& data,
                                 NES::Experimental::Statistics::StatisticCollectorIdentifierPtr statisticCollectorIdentifier,
                                 const uint64_t observedTuples,
                                 const uint64_t depth)
    : Statistic(std::move(statisticCollectorIdentifier), observedTuples, depth), data(data) {}

double ReservoirSample::probe(StatisticProbeParameterPtr&) {
    NES_ERROR("Not yet implemented!")
    return -1;
}

std::vector<uint64_t>& ReservoirSample::getData() { return data; }
}// namespace NES::Experimental::Statistics