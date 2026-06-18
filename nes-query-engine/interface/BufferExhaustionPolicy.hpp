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

namespace NES
{

/// Chooses which query is terminated to relieve global buffer-pool exhaustion. The "buffers held" by a query is
/// approximated by the sum of pending tasks across its pipelines (each in-flight task holds ~one pooled buffer).
enum class BufferExhaustionPolicy : uint8_t
{
    /// Terminate the running query holding the most buffers (default): one kill frees the most and targets the offender.
    TERMINATE_LARGEST,
    /// The query whose worker hit the exhaustion terminates itself (minimal; no enumeration; may be unfair).
    TERMINATE_SELF
};

}
