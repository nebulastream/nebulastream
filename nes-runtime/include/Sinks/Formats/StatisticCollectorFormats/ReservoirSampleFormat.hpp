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

#ifndef NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTORS_RESERVOIRSAMPLE_HPP_
#define NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTORS_RESERVOIRSAMPLE_HPP_

#include <Sinks/Formats/StatisticCollectorFormats/StatisticCollectorFormat.hpp>

namespace NES::Experimental::Statistics {

class Statistic;
using StatisticPtr = std::shared_ptr<Statistic>;

/**
 * @brief implements the ReservoirSampleFormat that is then written to the statisticStorage and potentially probed from there
 */
class ReservoirSampleFormat : public StatisticCollectorFormat {
  public:
    ReservoirSampleFormat() = default;

    virtual ~ReservoirSampleFormat() = default;

    /**
     * @brief defines how reservoir tuples are read from a buffer and creates samples from it
     * @param dynBuffer a dynamic tuple buffer that holds one or more samples
     * @return a vector of reservoir samples
     */
    std::vector<StatisticPtr> readFromBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& dynBuffer);
};
}// namespace NES::Experimental::Statistics
#endif//NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTORS_RESERVOIRSAMPLE_HPP_
