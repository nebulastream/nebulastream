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
#ifndef NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_SYNOPSES_SKETCHES_HPP
#define NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_SYNOPSES_SKETCHES_HPP

#include <memory>
#include "StatManager/StatCollectors/StatCollector.hpp"
#include <StatManager/StatCollectors/StatCollectorConfiguration.hpp>

namespace NES::Experimental::Statistics {

  class Sketch : public StatCollector {

  public:
    [[nodiscard]]uint32_t getDepth() const;
    [[nodiscard]]uint32_t getWidth() const;
    Sketch(const StatCollectorConfig& config, const uint32_t depth, const uint32_t width);
    void update(uint32_t key) override = 0;
    bool equal(const std::unique_ptr<StatCollector>& rightSketch, bool statCollection) override = 0;
    std::unique_ptr<StatCollector> merge(std::unique_ptr<StatCollector> rightSketch, bool statCollection) override = 0;
  private:
    uint32_t depth;
    uint32_t width;
  };

} // NES::Experimental::Statistics

#endif //NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_SYNOPSES_SKETCHES_HPP
