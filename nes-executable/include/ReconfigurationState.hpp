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
#include <ostream>
#include <set>
#include <string>
#include <unordered_map>
#include <Identifiers/Identifiers.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Util/Logger/Formatter.hpp>
#include <folly/Synchronized.h>

namespace NES
{

/// How a network sink ends its channel when its query terminates.
enum class ChannelTermination : uint8_t
{
    /// End the stream.
    Close,
    /// The receiving query is drained as well: send the reconfiguration marker, then end the stream.
    ForwardMarker,
    /// The receiving query keeps running: leave the channel without ending the stream, so a new sender can attach to it.
    Detach,
};

/// The reconfiguration state of one local query (ISQD). The worker records the reconfiguration markers that reach the query, the
/// pipelines of the query read the state through the PipelineExecutionContext. In the original, the marker is propagated through the
/// pipelines as a reconfiguration message; the current query engine terminates the pipelines of a query when its sources end, so the
/// pipelines look up the marker that ended the query instead. All members are thread-safe.
class ReconfigurationState
{
public:
    explicit ReconfigurationState(LocalQueryId localQueryId);

    /// Records a marker that reached the query, either injected by the coordinator or received by a network source. Returns the event of
    /// the query, or nothing if the marker has no event for the query or was already recorded (a marker may reach a query more than once).
    /// An update event provides its network sink updates, a drain or update-and-drain event drains the query.
    std::optional<ReconfigurationMetadata> recordMarker(const ReconfigurationMarker& marker);

    /// The marker that drains the query, if any.
    [[nodiscard]] std::optional<ReconfigurationMarker> getDrainMarker() const;

    /// True if the query is drained and hands the state of its stateful operators over to a state migration query.
    [[nodiscard]] bool isUpdateAndDrain() const;

    /// How the network sink of the channel ends the channel when the query terminates.
    [[nodiscard]] ChannelTermination getChannelTermination(const std::string& channelId) const;

    /// Takes the pending update of the network sink of the channel, if any.
    [[nodiscard]] std::optional<NetworkSinkUpdate> takeSinkUpdate(const std::string& channelId);

    [[nodiscard]] const LocalQueryId& getLocalQueryId() const { return localQueryId; }

private:
    struct Internal
    {
        std::set<std::string> recordedMarkers;
        std::optional<ReconfigurationMarker> drainMarker;
        std::optional<ReconfigurationMetadata> drainEvent;
        /// Pending network sink updates, keyed by the channel the sink currently sends over.
        std::unordered_map<std::string, NetworkSinkUpdate> sinkUpdates;
    };

    LocalQueryId localQueryId;
    folly::Synchronized<Internal> internal;
};

std::ostream& operator<<(std::ostream& os, ChannelTermination termination);

}

FMT_OSTREAM(NES::ChannelTermination);
