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

#ifndef NES_NES_EXECUTION_INCLUDE_UTIL_EXECUTION_HPP_
#define NES_NES_EXECUTION_INCLUDE_UTIL_EXECUTION_HPP_

#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeMeasure.hpp>
#include <Operators/LogicalOperators/Windows/Types/TimeBasedWindowType.hpp>
#include <cstdint>
#include <tuple>

namespace NES::QueryCompilation::Util {

/**
 * @brief Get the windowing parameter (size, slide, and time function) for the given window type
 * @param windowType
 * @return Tuple<WindowSize, WindowSlide, TimeFunction>
 */
std::tuple<uint64_t, uint64_t, Runtime::Execution::Operators::TimeFunctionPtr>
    getWindowingParameters(Windowing::TimeBasedWindowType& windowType);

} // namespace NES::QueryCompilation::Util

#endif//NES_NES_EXECUTION_INCLUDE_UTIL_EXECUTION_HPP_
