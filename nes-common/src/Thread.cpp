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

#include <Thread.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <string>
#include <Identifiers/Identifiers.hpp>

namespace NES
{

thread_local Host Thread::WorkerNodeId = Host("Not A Worker");
thread_local std::string Thread::ThreadName = "unnamed";

namespace detail
{

thread_local std::unordered_map<std::type_index, std::function<void()>> threadInitHooks;
thread_local bool isNESThread = false;

constexpr size_t PTHREAD_NAME_LENGTH = 15;

void applyThreadInitHooks(std::unordered_map<std::type_index, std::function<void()>> hooks)
{
    threadInitHooks = std::move(hooks);
    for (const auto& [key, hook] : threadInitHooks)
    {
        hook();
    }
}

void setPthreadName(std::string_view name)
{
    std::array<char, PTHREAD_NAME_LENGTH + 1> truncated{};
    std::copy_n(name.begin(), std::min(PTHREAD_NAME_LENGTH, name.size()), truncated.begin());
    pthread_setname_np(pthread_self(), truncated.data());
}

}

void setCurrentThreadName(std::string name)
{
    detail::setPthreadName(name);
    Thread::ThreadName = std::move(name);
}

}
