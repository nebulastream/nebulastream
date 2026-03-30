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

#include <Async/IOThread.hpp>

#include <boost/asio/executor_work_guard.hpp>

#include <Util/Logger/Logger.hpp>

namespace NES
{

/// The io_context is initialized first from its default constructor.
IOThread::IOThread(size_t poolSize) : ioContexts(poolSize), nextContext(0)
{
    NES_DEBUG("IOThread: starting with {} threads.", poolSize);

    // Create one io_context per thread
    for (size_t i = 0; i < poolSize; ++i)
    {
        // Create work guard to keep io_context running
        workGuards.emplace_back(asio::make_work_guard(ioContexts.at(i)));

        // Create thread that runs this specific io_context
        threads.emplace_back(fmt::format("IOThread_{}", i), [ioc = &ioContexts.at(i)]() { ioc->run(); });
    }
}

IOThread::~IOThread()
{
    NES_DEBUG("IOThread: stopping all contexts.");

    // Reset all work guards to allow io_contexts to exit
    workGuards.clear();

    // Stop all io_contexts
    for (auto& ioc : ioContexts)
    {
        ioc.stop();
    }

    // Threads will join automatically due to jthread
    NES_DEBUG("IOThread: stopped.");
}

}
