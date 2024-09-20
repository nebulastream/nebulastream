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

#include <Reconfiguration/ReconfigurationMarkerEvent.hpp>

namespace NES {

ReconfigurationMarkerEventPtr ReconfigurationMarkerEvent::create(const QueryState queryState,
                                                                 const ReconfigurationMetadatatPtr& reconfigurationMetadata) {
    return std::make_shared<ReconfigurationMarkerEvent>(queryState, reconfigurationMetadata);
}

ReconfigurationMarkerEvent::ReconfigurationMarkerEvent(const QueryState queryState,
                                                       const ReconfigurationMetadatatPtr& reconfigurationMetadata)
    : queryState(queryState), reconfigurationMetadata(std::move(reconfigurationMetadata)) {}

}// namespace NES
