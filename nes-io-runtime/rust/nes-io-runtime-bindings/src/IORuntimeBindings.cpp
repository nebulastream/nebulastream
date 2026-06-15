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

#include <IORuntimeBindings.hpp>

#include <cstddef>
#include <string>

#include <nes-io-runtime-bindings/lib.h>
#include <rust/cxx.h>
#include <Thread.hpp>

void init_thread(std::shared_ptr<ThreadInitializationContext> context)
{
    PRECONDITION(context != nullptr, "context must not be null");
    NES::Thread::initializeThread(context->host, fmt::format("IO-{}", context->counter++));
    for (const auto& hook : context->threadInitHooks)
    {
        hook();
    }
}
