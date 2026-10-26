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
::NES::EventTrace::Key toKey(const Key& key)
{
    return {.origin = key.origin, .seq = key.seq, .chunk = key.chunk};
}

::NES::EventTrace::Stage toStage(Stage stage)
{
    return static_cast<::NES::EventTrace::Stage>(static_cast<uint16_t>(stage));
}

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

uint64_t now() noexcept
{
    return ::NES::EventTrace::now();
}

void begin(Stage stage, const Key& key, uint64_t pipeline) noexcept
{
    ::NES::EventTrace::begin(toStage(stage), toKey(key), pipeline);
}

void end(Stage stage, const Key& key, uint64_t pipeline, uint64_t extra) noexcept
{
    ::NES::EventTrace::end(toStage(stage), toKey(key), pipeline, extra);
}

void mark(Stage stage, const Key& key, uint64_t pipeline, uint64_t time) noexcept
{
    ::NES::EventTrace::mark(toStage(stage), toKey(key), pipeline, time);
}
#else
std::unique_ptr<Counter> create_counter(rust::Str, rust::Box<Sample>)
{
    return std::make_unique<Counter>();
}

uint64_t now() noexcept
{
    return 0;
}

void begin(Stage, const Key&, uint64_t) noexcept
{
}

void end(Stage, const Key&, uint64_t, uint64_t) noexcept
{
}

void mark(Stage, const Key&, uint64_t, uint64_t) noexcept
{
}
#endif

}
