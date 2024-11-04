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

#ifndef NES_COMMON_INCLUDE_RECONFIGURATION_RECONFIGURATIONMARKEREVENT_HPP_
#define NES_COMMON_INCLUDE_RECONFIGURATION_RECONFIGURATIONMARKEREVENT_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Reconfiguration/Metadata/ReconfigurationMetadata.hpp>
#include <Util/QueryState.hpp>
#include <memory>

namespace NES {
class ReconfigurationMarkerEvent;
using ReconfigurationMarkerEventPtr = std::shared_ptr<const ReconfigurationMarkerEvent>;

/**
 * @brief Reconfiguration marker event represents a single reconfiguration instruction that defines what is to be done as part of
 * the reconfiguration and how to do so.
 *
 * The what is represented by the query state. We currently support Marker_For_Migration and Marked_For_Redeployment.
 * How the reconfiguration is to be done is defined by the reconfiguration metadata.
 */
class ReconfigurationMarkerEvent {
  public:
    static ReconfigurationMarkerEventPtr create(QueryState queryState, ReconfigurationMetadatatPtr reconfigurationMetadata);

    ReconfigurationMarkerEvent(QueryState queryState, ReconfigurationMetadatatPtr reconfigurationMetadata);

    const QueryState queryState;
    const ReconfigurationMetadatatPtr reconfigurationMetadata;
};
}// namespace NES
#endif// NES_COMMON_INCLUDE_RECONFIGURATION_RECONFIGURATIONMARKEREVENT_HPP_
