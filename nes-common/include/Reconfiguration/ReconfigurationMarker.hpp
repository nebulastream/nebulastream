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

#include <map>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

/// A network sink that connects to a new receiver (the original's NetworkSinkUpdateInfo).
struct NetworkSinkUpdate
{
    /// The channel the sink currently sends over. It identifies the sink within its plan.
    std::string oldChannel;
    std::string newChannel;
    /// Data address of the worker that receives the new channel.
    std::string newDataEndpoint;

    bool operator==(const NetworkSinkUpdate&) const = default;
};

/// Drain: flush the in-flight tuples, then terminate (the original's DrainQueryMetadata).
struct DrainQueryMetadata
{
    /// The outgoing channels whose receiving plan keeps running. Their network sinks detach instead of forwarding the marker, so a new
    /// sender can attach to the receiver. The other network sinks forward the marker.
    std::vector<std::string> detachedChannels;

    bool operator==(const DrainQueryMetadata&) const = default;
};

/// Update: switch to the new version of the plan (the original's UpdateQueryMetadata). The network sinks connect to their new receivers;
/// the old receivers get the marker.
struct UpdateQueryMetadata
{
    std::vector<NetworkSinkUpdate> networkSinkUpdates;

    bool operator==(const UpdateQueryMetadata&) const = default;
};

/// Update and drain: hand the state of the stateful operators over to the state migration query, then terminate (the original's
/// UpdateAndDrainQueryMetadata).
struct UpdateAndDrainQueryMetadata
{
    /// See DrainQueryMetadata::detachedChannels.
    std::vector<std::string> detachedChannels;

    bool operator==(const UpdateAndDrainQueryMetadata&) const = default;
};

using ReconfigurationMetadata = std::variant<DrainQueryMetadata, UpdateQueryMetadata, UpdateAndDrainQueryMetadata>;

/// The reconfiguration marker of ISQD: a map from decomposed query plans to the reconfiguration they perform. The marker is injected at the
/// affected upstream plans and flows downstream through the data channels like a punctuation. Every plan looks up its own event.
///
/// Deviation: the original keys the events by decomposed query plan id and version. The workers of the current code base only know the
/// ids of their local queries, so the events are keyed by local query id. The coordinator translates its decomposed query plans to local
/// queries when it builds the marker.
class ReconfigurationMarker
{
public:
    /// Creates a marker with a new unique id.
    static ReconfigurationMarker create();

    [[nodiscard]] const std::string& getId() const { return id; }

    [[nodiscard]] std::optional<ReconfigurationMetadata> getReconfigurationEvent(const LocalQueryId& localQueryId) const;
    void addReconfigurationEvent(const LocalQueryId& localQueryId, ReconfigurationMetadata event);

    [[nodiscard]] const std::map<std::string, ReconfigurationMetadata>& getAllReconfigurationEvents() const { return events; }

    [[nodiscard]] std::string serialize() const;
    /// Throws a CannotDeserialize exception if the input is not a serialized marker.
    static ReconfigurationMarker deserialize(std::string_view serialized);

    bool operator==(const ReconfigurationMarker&) const = default;
    friend std::ostream& operator<<(std::ostream& os, const ReconfigurationMarker& marker);

private:
    explicit ReconfigurationMarker(std::string id);

    std::string id;
    /// Keyed by the string representation of the local query id.
    std::map<std::string, ReconfigurationMetadata> events;
};

}

FMT_OSTREAM(NES::ReconfigurationMarker);
