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
#include <optional>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace NES
{

/// Returns the maximum watermark gap if a window-based operator with these input origins votes for backpressure, i.e., if watermark
/// backpressure is enabled and the operator has several input origins. Returns nothing otherwise.
inline std::optional<uint64_t> watermarkGapThrottleOf(const std::vector<OriginId>& inputOrigins, const QueryExecutionConfiguration& conf)
{
    if (conf.maxWatermarkGapInMs == 0 || inputOrigins.size() <= 1)
    {
        return std::nullopt;
    }
    return conf.maxWatermarkGapInMs;
}

}
