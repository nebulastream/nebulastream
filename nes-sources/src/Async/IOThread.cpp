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

#include <cstdlib>
#include <pthread.h>
#include <sched.h>
#include <boost/asio/executor_work_guard.hpp>

#include <Util/CcxTopology.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

/// The io_context is initialized first from its default constructor.
IOThread::IOThread(const bool pinThreads, const size_t poolSize) : ioContexts(poolSize), nextContext(0)
{
    NES_DEBUG("IOThread: starting with {} threads, and thread pinning: {}.", poolSize, pinThreads);

    for (size_t i = 0; i < poolSize; ++i)
    {
        if (pinThreads)
        {
            workGuards.emplace_back(asio::make_work_guard(ioContexts.at(i)));

            /// NES_PIN_CPU_OFFSET shifts the pin base CPU (default 0 = CCX0). Set e.g. 6 to pin the engine
            /// onto CCX1 (cpu 6-11) -- used to test whether pinning's TCP penalty is cpu0/CCX0-softirq-specific.
            /// With CCX-aware task queues, io threads are STRIPED across the CCXs instead (one
            /// cell of io feed + shard + owner workers per CCX; NES_CCX_PIN_LAYOUT=compact reverts).
            const char* const pinOffsetEnv = std::getenv("NES_PIN_CPU_OFFSET");
            const size_t pinOffset = pinOffsetEnv ? std::strtoul(pinOffsetEnv, nullptr, 10) : 0;
            const size_t pinCpu = CcxAffinity::stripedLayout() ? CcxTopology::instance().ioThreadCpu(i) : pinOffset + i;
            threads.emplace_back(
                fmt::format("IOThread_{}", i),
                [ioc = &ioContexts.at(i), cpuId = pinCpu]()
                {
                    // Pin this thread to a specific CPU core
                    cpu_set_t cpuset;
                    CPU_ZERO(&cpuset);
                    CPU_SET(cpuId, &cpuset);

                    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
                    if (rc != 0)
                    {
                        NES_WARNING("IOThread: failed to pin thread to CPU {}: error {}", cpuId, rc);
                    }
                    else
                    {
                        NES_DEBUG("IOThread: pinned thread to CPU {}", cpuId);
                        /// Tag the thread's CCX so CCX-aware task queues route this io thread's
                        /// emitted buffers to workers sharing its L3 (no-op when sharding is off).
                        CcxAffinity::ccxId = CcxTopology::instance().ccxOf(cpuId);
                    }

                    ioc->run();
                });
        }
        else
        {
            // Create work guard to keep io_context running
            workGuards.emplace_back(asio::make_work_guard(ioContexts.at(i)));

            // Create thread that runs this specific io_context
            threads.emplace_back(fmt::format("IOThread_{}", i), [ioc = &ioContexts.at(i)]() { ioc->run(); });
        }
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
