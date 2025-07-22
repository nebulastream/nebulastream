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

/// Configuration options for the network services (sender and receiver).
/// Passed to initNetworkServices() to configure per-worker defaults.
struct NetworkOptions
{
    /// Size of the sender software queue per channel.
    uint32_t senderQueueSize = 1024;
    /// Maximum number of in-flight buffers awaiting acknowledgment per channel.
    uint32_t maxPendingAcks = 64;
    /// Size of the receiver data queue per channel.
    uint32_t receiverQueueSize = 10;
    /// Number of IO threads for the sender tokio runtime. 0 means use the number of available cores.
    uint32_t senderIOThreads = 1;
    /// Number of IO threads for the receiver tokio runtime. 0 means use the number of available cores.
    uint32_t receiverIOThreads = 1;
};

}
