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

    /**
     * Some questions:
     *    - How can I make sure that the left and right stream have seen everything.
     *    - Is the originId unique for each logical stream? So can I have the same originId for the left and right stream?
     *    - how do I know what the left and right originId are? Do I even need it?
     *    - Do I need a SliceTriggerChecker for left and right stream? Or is one sufficient?
     *    - How do I know what slices/windows are open?
     *        - Do I need to implement some callback function or something like that?
     *        - What information about the open windows do I really need?
     *    - Regarding the slices/windows: Can I maybe use the sliceassigner in the SiceTriggerChecker to have an easy way of getting the start and end of a given slice?
     *    - We can say that this gets called after every buffer and therefore, we can say that the slices/windows have to be given as a parameter...
     *        - If we do this, how do we want them? Reference of a vector?
     *
     * @return
     */
    std::vector<SliceIdentifier> updateAndGetTriggeredSlices();

  private:
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessor;
    std::vector<Timestamp> workerLocalLastWatermark;
};
} // namespace NES::Runtime::Execution::Operators

#endif //NES_SLICETRIGGERCHECKER_HPP
