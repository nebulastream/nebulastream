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

#include <algorithm>
#include <array>
#include <cstddef>
#include <string_view>
#include <pthread.h>
#include <Util/ThreadNaming.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
void setThreadName(const std::string_view threadName)
{
    PRECONDITION(!threadName.empty(), "Thread name cannot be empty");
    std::array<char, detail::PTHREAD_NAME_LENGTH + 1> truncatedStringName{};
    std::copy_n(threadName.begin(), std::min(detail::PTHREAD_NAME_LENGTH, threadName.size()), truncatedStringName.begin());
    pthread_setname_np(pthread_self(), truncatedStringName.data());
}
}
