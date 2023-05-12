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

#ifndef NES_SLICETRIGGERCHECKER_HPP
#define NES_SLICETRIGGERCHECKER_HPP

#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>

namespace NES::Runtime::Execution::Operators {

// TODO think about, if it makes sense to encapsulates this into a separate class
using SliceIdentifier = uint64_t;

using Timestamp = uint64_t;

class SliceTriggerChecker {

  public:
    std::vector<SliceIdentifier> updateAndGetTriggeredSlices();

  private:
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessor;
    std::vector<Timestamp> workerLocalLastWatermark;
};
} // namespace NES::Runtime::Execution::Operators

#endif //NES_SLICETRIGGERCHECKER_HPP
