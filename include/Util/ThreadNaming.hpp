/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_UTIL_THREADNAMING_HPP_
#define NES_INCLUDE_UTIL_THREADNAMING_HPP_

namespace NES {
/**
 * @brief Sets the calling thread's name using the supplied
 * formattable string. For example, setThreadName("helper") will
 * set the thread name to "helper", setThreadName("helper-%d", 123)
 * will set the thread name to "helper-123". Be careful that on some
 * operating systems, the length of the thread name is constrained, e.g.,
 * on Linux it is 16 characters.
 * @param threadNameFmt name of the thread with formatting option
 * @param ... variadic arguments
 */
void setThreadName(const char* threadNameFmt, ...);
}// namespace NES

#endif//NES_INCLUDE_UTIL_THREADNAMING_HPP_
