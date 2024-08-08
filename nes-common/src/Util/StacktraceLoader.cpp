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

#include <Util/Logger/Logger.hpp>
#include <Util/StacktraceLoader.hpp>
#include <cpptrace/cpptrace.hpp>
#define CALLSTACK_MAX_SIZE 32

namespace NES
{
/**
 * @brief This method collects the call stacks and prints it
 */
std::string collectAndPrintStacktrace()
{
    auto stack = cpptrace::generate_trace(1).to_string();
    NES_ERROR("Stacktrace:\n{}", stack);
    return stack;
}
/**
 * @brief This method only collects the call stacks
 * Use this instead, if the Stacktrace is given over to another function
 * to construct and print an error message, i.e. 'OnFatalError()'
 * to avoid duplicate printing of the stack
 */
std::string collectStacktrace()
{
    return cpptrace::generate_trace(1).to_string();
}

} /// namespace NES
