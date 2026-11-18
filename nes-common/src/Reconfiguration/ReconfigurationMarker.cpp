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

#include <Reconfiguration/ReconfigurationMarker.hpp>

#include <map>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Util/UUID.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <rfl/AddTagsToVariants.hpp>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// The serialized form of a marker.
struct SerializedReconfigurationMarker
{
    std::string id;
    std::map<std::string, ReconfigurationMetadata> events;
};
}

ReconfigurationMarker::ReconfigurationMarker(std::string id) : id(std::move(id))
{
}

ReconfigurationMarker ReconfigurationMarker::create()
{
    return ReconfigurationMarker(UUIDToString(generateUUID()));
}

std::optional<ReconfigurationMetadata> ReconfigurationMarker::getReconfigurationEvent(const LocalQueryId& localQueryId) const
{
    if (const auto it = events.find(localQueryId.getRawValue()); it != events.end())
    {
        return it->second;
    }
    return std::nullopt;
}

void ReconfigurationMarker::addReconfigurationEvent(const LocalQueryId& localQueryId, ReconfigurationMetadata event)
{
    events.insert_or_assign(localQueryId.getRawValue(), std::move(event));
}

std::string ReconfigurationMarker::serialize() const
{
    return rfl::json::write<rfl::AddTagsToVariants>(SerializedReconfigurationMarker{.id = id, .events = events});
}

ReconfigurationMarker ReconfigurationMarker::deserialize(const std::string_view serialized)
{
    auto result = rfl::json::read<SerializedReconfigurationMarker, rfl::AddTagsToVariants>(serialized);
    if (not result)
    {
        throw CannotDeserialize("Invalid reconfiguration marker: {}", result.error().what());
    }
    ReconfigurationMarker marker(std::move(result->id));
    marker.events = std::move(result->events);
    return marker;
}

std::ostream& operator<<(std::ostream& os, const ReconfigurationMarker& marker)
{
    return os << fmt::format(
               "ReconfigurationMarker(id: {}, events for: [{}])", marker.id, fmt::join(marker.events | std::views::keys, ", "));
}

}
