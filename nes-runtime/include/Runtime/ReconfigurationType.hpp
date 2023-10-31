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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_RECONFIGURATIONTYPE_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_RECONFIGURATIONTYPE_HPP_

#include <cstdint>

namespace NES::Runtime {
enum class ReconfigurationType : uint8_t {
    /// use Initialize for reconfiguration tasks that initialize a reconfigurable instance
    Initialize,
    /// use Destroy for reconfiguration tasks that cleans up a reconfigurable instance
    Destroy,
    /// graceful stop of a query
    SoftEndOfStream,
    /// forceful stop of a query without a failure
    HardEndOfStream,
    /// forceful stop of a query with a failure
    FailEndOfStream,
    /// use PropagateEpoch to pass epoch barrier to all network sinks
    PropagateEpoch,
    /// start the process of connecting to a new network source
    ConnectToNewReceiver,
    /// indicate successful establishment of a network connection
    ConnectionEstablished,
    /// indicates that the old version of a query has been drained, and can be reconfigured or stopped depending on the situation
    DrainVersion
};
}

#endif// NES_RUNTIME_INCLUDE_RUNTIME_RECONFIGURATIONTYPE_HPP_
