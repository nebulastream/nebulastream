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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACE_FUNCTIONCALLTARGET_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACE_FUNCTIONCALLTARGET_HPP_
#include <ostream>
#include <string>
namespace NES::Nautilus::Tracing {

/**
 * @brief Captures a function call in the trace.
 */
class FunctionCallTarget {
  public:
    FunctionCallTarget(const std::string& mangledFunctionName, void* functionPtr);
    std::string mangledName;
    void* functionPtr;
    friend std::ostream& operator<<(std::ostream& os, const FunctionCallTarget& target);
};

}// namespace NES::Nautilus::Tracing

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TRACE_FUNCTIONCALLTARGET_HPP_
