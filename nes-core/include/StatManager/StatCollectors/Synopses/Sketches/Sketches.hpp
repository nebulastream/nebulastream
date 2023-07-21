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

#include "StatManager/StatCollectors/StatCollector.hpp"

namespace NES {

  class Sketches : public StatCollector {

  public:
    [[nodiscard]]uint32_t getDepth() const;
    [[nodiscard]]uint32_t getWidth() const;
    void setDepth(uint32_t depth);
    void setWidth(uint32_t width);
    Sketches(const std::string& physicalSourceName, const std::string& field,
             time_t duration, time_t interval);
    void update(uint32_t key) override = 0;
    sketch* merge(sketch* rightSketch) override = 0;
    bool equal(sketch* otherSketch) override = 0;
  private:
    uint32_t depth;
    uint32_t width;
  };

} // NES

#endif //NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_SYNOPSES_SKETCHES_HPP
