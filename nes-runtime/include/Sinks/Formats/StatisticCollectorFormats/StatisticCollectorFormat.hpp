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

#ifndef NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTORS_STATISTICCOLLECTOR_HPP_
#define NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTORS_STATISTICCOLLECTOR_HPP_

#include <Identifiers.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/StatisticCollectorType.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace NES {

namespace Runtime::MemoryLayouts {
class DynamicTupleBuffer;
}

namespace Experimental::Statistics {

class Statistic;
using StatisticPtr = std::shared_ptr<Statistic>;

/**
 * @brief the abstract class that just defines the format that is read from a buffer for a statistic tuple
 */
class StatisticCollectorFormat {
  public:
    StatisticCollectorFormat() = default;

    virtual ~StatisticCollectorFormat() = default;

    /**
     * @brief defines how we read from the dynamic buffer and also does so
     * @param dynBuffer the buffer which we wish to read
     * @param logicalSourceName the logicalSourceName over which the source was built
     * @return a vector of statistic objects
     */
    virtual std::vector<StatisticPtr> readFromBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& dynBuffer, const std::string& logicalSourceName) = 0;
};
}// namespace Experimental::Statistics
}// namespace NES
#endif//NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTORS_STATISTICCOLLECTOR_HPP_
