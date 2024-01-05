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

#include <Execution/Operators/Streaming/SliceAssigner.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES::Runtime::Execution::Operators {

SliceAssigner::SliceAssigner(uint64_t windowSize, uint64_t windowSlide) : windowSize(windowSize), windowSlide(windowSlide) {
    NES_ASSERT(windowSize >= windowSlide,
               "Currently the window assigner dose not support windows with a larger slide then the window size.");
}

}// namespace NES::Runtime::Execution::Operators