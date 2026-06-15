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

#include <IORuntime.hpp>

#include <nes-io-runtime-bindings/lib.h>
#include <IORuntimeBindings.hpp>
#include <Thread.hpp>

namespace NES
{

IORuntime::IORuntime() : handle(init_io_runtime(2, ThreadInitializationContext::fromCurrentThreadsContext()))
{
}

IORuntime::~IORuntime() = default;

void IORuntime::attachConfig(std::string_view serviceName, std::string_view config)
{
    attach_config(*handle, rust::Str(std::string(serviceName)), rust::Str(std::string(config)));
}

} /// namespace NES

extern "C" IORuntimeHandle* current_io_runtime_internal()
{
    if (auto instance = NES::IORuntime::tryInstance())
    {
        return std::addressof(*instance->handle);
    }
    NES_ERROR("No IORuntime instance found.");
    return nullptr;
}
