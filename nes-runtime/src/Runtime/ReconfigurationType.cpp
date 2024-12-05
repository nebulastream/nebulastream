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
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/ReconfigurationType.hpp>
namespace NES::Runtime {
QueryTerminationType reconfigurationTypeToTerminationType(ReconfigurationType reconfigurationType) {
    switch (reconfigurationType) {
        case ReconfigurationType::SoftEndOfStream: return QueryTerminationType::Graceful;
        case ReconfigurationType::HardEndOfStream: return QueryTerminationType::HardStop;
        case ReconfigurationType::FailEndOfStream: return QueryTerminationType::Failure;
        case ReconfigurationType::ReconfigurationMarker: return QueryTerminationType::Reconfiguration;
        default: {
            NES_ASSERT(false, "Cannot convert this reconfiguration type to a termination type");
        }
    }
}
}// namespace NES::Runtime
