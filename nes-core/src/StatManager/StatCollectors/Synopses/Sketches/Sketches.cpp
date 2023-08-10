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

#include "StatManager/StatCollectors/Synopses/Sketches/Sketches.hpp"

namespace NES::Experimental::Statistics {

  uint32_t Sketch::getDepth() const {
    return this->depth;
  }

  uint32_t Sketch::getWidth() const {
    return this->width;
  }

  Sketch::Sketch(const StatCollectorConfig& config,
                 const uint32_t depth, const uint32_t width)
                     : StatCollector(config), depth(depth), width(width) {

  }

} // NES::Experimental::Statistics