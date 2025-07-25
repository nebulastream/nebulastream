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

#pragma once
#include <cstddef>
#include <string_view>

namespace NES
{
namespace detail
{
///POSIX limits the thread name to 16 bytes which includes null termination:
///https://man7.org/linux/man-pages/man3/pthread_setname_np.3.html
constexpr size_t PTHREAD_NAME_LENGTH = 15;
}

///Sets the currents thread's name.
///threadName has to be non-empty and will be truncated to PTHREAD_NAME_LENGTH character
void setThreadName(std::string_view threadName);
}
