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
ReconfigurationType terminationTypeToReconfigurationType(QueryTerminationType terminationType) {
    switch (terminationType) {
        case QueryTerminationType::Graceful: return ReconfigurationType::SoftEndOfStream;
        case QueryTerminationType::HardStop: return ReconfigurationType::HardEndOfStream;
        case QueryTerminationType::Failure: return ReconfigurationType::FailEndOfStream;
        case QueryTerminationType::Reconfiguration: return ReconfigurationType::ReconfigurationMarker;
        default: {
            NES_ASSERT(false, "Cannot convert this termination type to a reconfiguration type");
        }
    }
}
}// namespace NES::Runtime
