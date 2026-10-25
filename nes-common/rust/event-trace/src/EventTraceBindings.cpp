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

#include <EventTraceBindings.hpp>

#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <Util/EventTrace.hpp>
#include <nes-event-trace-bindings/lib.h>
#include <rust/cxx.h>

namespace NES::EventTrace::Rust
{

#ifdef NES_EVENT_TRACE_ENABLED
namespace
{
class TracedCounter final : public Counter
{
public:
    TracedCounter(rust::Str name, rust::Box<Sample> sample)
        : sample(std::move(sample)), updater(std::string_view(name.data(), name.size()), [this] { return Rust::sample(*this->sample); })
    {
    }

private:
    rust::Box<Sample> sample;
    /// Declared after the sample it calls, so it stops sampling before the sample is dropped.
    CounterUpdater updater;
};
}

std::unique_ptr<Counter> create_counter(rust::Str name, rust::Box<Sample> sample)
{
    return std::make_unique<TracedCounter>(name, std::move(sample));
}

void net_step(NetStep step, uint64_t origin, uint64_t seq, uint64_t chunk, uint64_t channel) noexcept
{
    const Key key{.origin = origin, .seq = seq, .chunk = chunk};
    switch (step)
    {
        case NetStep::Dequeue:
            instant(NetDequeue{.key = key, .channel = channel});
            return;
        case NetStep::Encoded:
            instant(NetEncoded{.key = key, .channel = channel});
            return;
        case NetStep::Acked:
            instant(NetAcked{.key = key, .channel = channel});
            return;
        case NetStep::Decoded:
            instant(NetDecoded{.key = key, .channel = channel});
            return;
        case NetStep::Queued:
            instant(NetQueued{.key = key, .channel = channel});
            return;
    }
}

#else
std::unique_ptr<Counter> create_counter(rust::Str, rust::Box<Sample>)
{
    return std::make_unique<Counter>();
}

void net_step(NetStep, uint64_t, uint64_t, uint64_t, uint64_t) noexcept
{
}

#endif

}
