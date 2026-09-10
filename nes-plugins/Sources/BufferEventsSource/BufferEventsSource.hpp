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

#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Sources/EventFeedSource.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

/// Reads what the worker's buffer providers are doing, which the worker's 'BufferStatisticListener'
/// publishes as one CSV row per flush interval. Buffer pool pressure thereby becomes something
/// NebulaStream can query itself. See 'EventFeedSource' for how a feed reaches a query.
///
/// Unlike the task events, the rows of this feed are a regular time series rather than one row per event,
/// so a query reading it cannot amplify its own buffer usage into more rows.
class BufferEventsSource final : public EventFeedSource
{
public:
    constexpr static std::string_view NAME = "BufferEvents";

    explicit BufferEventsSource(const SourceDescriptor& sourceDescriptor);

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);
};

struct ConfigParametersBufferEvents
{
    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, EventFeedSourceConfig::FLUSH_INTERVAL_MS);
};

}
