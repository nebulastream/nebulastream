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

#include <cstdint>
#include <memory>
#include <string>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <BackpressureChannel.hpp>

namespace NES
{

class SinkProvider
{
public:
    explicit SinkProvider(uint32_t defaultChannelCapacity = 64);
    std::unique_ptr<Sink> lower(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor) const;
    [[nodiscard]] bool contains(const std::string& sinkType) const;

private:
    uint32_t defaultChannelCapacity;
};

/// Compatibility wrapper -- delegates to SinkProvider with default channel capacity.
/// Existing call sites use this free function; new code should prefer SinkProvider directly.
std::unique_ptr<Sink> lower(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);

}
