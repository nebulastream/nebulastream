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

#ifndef NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTORS_COUNTMIN_HPP_
#define NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTORS_COUNTMIN_HPP_

#include <Sinks/Formats/StatisticCollectorFormats/StatisticCollectorFormat.hpp>

namespace NES::Experimental::Statistics {

class Statistic;
using StatisticPtr = std::shared_ptr<Statistic>;

/**
 * @brief implements the CountMinFormat sketch that is then written to the statisticStorage and potentially probed from there
 */
class CountMinFormat : public StatisticCollectorFormat {
  public:
    CountMinFormat() = default;

    virtual ~CountMinFormat() = default;

    /**
     * @brief defines how CountMin tuples are read from a buffer and creates CountMin sketches from it
     * @param dynBuffer a dynamic tuple buffer that holds one or more sketches
     * @param logicalSourceName the name of the logicalSource whose format we will read
     * @return a vector of CountMin sketches
     */
    std::vector<StatisticPtr> readFromBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& dynBuffer, const std::string& logicalSourceName) override;

};
}
#endif//NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTORS_COUNTMIN_HPP_
