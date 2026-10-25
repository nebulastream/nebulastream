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

#include <Util/EventTrace.hpp>

#ifdef NES_EVENT_TRACE_ENABLED

    #include <chrono>
    #include <cstdint>
    #include <cstdlib>
    #include <functional>
    #include <mutex>
    #include <optional>
    #include <stop_token>
    #include <string>
    #include <string_view>
    #include <thread>
    #include <utility>
    #include <vector>
    #include <EventTraceBackends.hpp>

namespace NES::EventTrace
{

void record(uint16_t type, Kind kind, const void* payload, uint32_t size) noexcept
{
    #ifdef NES_EVENT_TRACE_RING
    Ring::record(type, kind, payload, size);
    #endif
    #ifdef NES_EVENT_TRACE_ITT
    Itt::record(type, kind, payload, size);
    #endif
}

void writePlan([[maybe_unused]] std::string_view plan)
{
    #ifdef NES_EVENT_TRACE_RING
    Ring::writePlan(plan);
    #endif
}

namespace
{
constexpr auto SAMPLE_INTERVAL = std::chrono::milliseconds(1);

/// Samples all counters while holding the lock, so a CounterUpdater's destructor cannot return during its sample.
class Sampler
{
public:
    uint64_t add(std::string_view name, std::function<int64_t()> sample)
    {
        const std::scoped_lock lock(mutex);
        const auto id = nextId++;
        counters.push_back({.id = id, .sample = std::move(sample), .last = std::nullopt});
    #ifdef NES_EVENT_TRACE_RING
        Ring::counterCreated(id, name);
    #endif
    #ifdef NES_EVENT_TRACE_ITT
        Itt::counterCreated(id, name);
    #endif
        if (!thread.joinable())
        {
            thread = std::jthread([this](const std::stop_token& stopToken) { run(stopToken); });
        }
        return id;
    }

    void remove(uint64_t id)
    {
        const std::scoped_lock lock(mutex);
        std::erase_if(counters, [id](const auto& counter) { return counter.id == id; });
    #ifdef NES_EVENT_TRACE_ITT
        Itt::counterDestroyed(id);
    #endif
    }

private:
    struct Counter
    {
        uint64_t id;
        std::function<int64_t()> sample;
        std::optional<int64_t> last;
    };

    void run(const std::stop_token& stopToken)
    {
        while (!stopToken.stop_requested())
        {
            std::this_thread::sleep_for(SAMPLE_INTERVAL);
            const std::scoped_lock lock(mutex);
            for (auto& counter : counters)
            {
                const auto value = counter.sample();
                if (value == counter.last)
                {
                    continue;
                }
                counter.last = value;
                instant(CounterSample{.counter = counter.id, .value = value});
            }
        }
    }

    std::mutex mutex;
    std::vector<Counter> counters;
    uint64_t nextId = 1;
    /// Declared last, so it stops before the members it uses are destroyed.
    std::jthread thread;
};

Sampler& sampler()
{
    static Sampler instance;
    return instance;
}
}

CounterUpdater::CounterUpdater(std::string_view name, std::function<int64_t()> sample) : id(sampler().add(name, std::move(sample)))
{
}

CounterUpdater::~CounterUpdater()
{
    sampler().remove(id);
}

bool traces(std::string_view queryDescription)
{
    static const std::string filter = []
    {
        const auto* env = std::getenv("NES_LATENCY_TRACE_QUERY"); /// NOLINT(concurrency-mt-unsafe) read once during initialization
        return env != nullptr ? std::string(env) : std::string();
    }();
    return queryDescription.contains(filter);
}

}

#endif
