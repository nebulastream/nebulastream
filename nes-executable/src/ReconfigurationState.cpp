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

#include <ReconfigurationState.hpp>

#include <algorithm>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <magic_enum/magic_enum.hpp>

namespace NES
{

ReconfigurationState::ReconfigurationState(LocalQueryId localQueryId) : localQueryId(std::move(localQueryId))
{
}

std::optional<ReconfigurationMetadata> ReconfigurationState::recordMarker(const ReconfigurationMarker& marker)
{
    auto event = marker.getReconfigurationEvent(localQueryId);
    if (not event.has_value())
    {
        return std::nullopt;
    }
    auto locked = internal.wlock();
    if (not locked->recordedMarkers.insert(marker.getId()).second)
    {
        return std::nullopt;
    }
    std::visit(
        Overloaded{
            [&](const UpdateQueryMetadata& update)
            {
                for (const auto& sinkUpdate : update.networkSinkUpdates)
                {
                    const auto handler = locked->sinkUpdateHandlers.find(sinkUpdate.oldChannel);
                    if (handler == locked->sinkUpdateHandlers.end())
                    {
                        locked->pendingSinkUpdates.insert_or_assign(sinkUpdate.oldChannel, std::pair{sinkUpdate, marker});
                        continue;
                    }
                    auto [owner, sinkHandler] = std::move(handler->second);
                    locked->sinkUpdateHandlers.erase(handler);
                    const auto newChannel = sinkHandler(sinkUpdate, marker);
                    locked->sinkUpdateHandlers.insert_or_assign(newChannel, std::pair{owner, std::move(sinkHandler)});
                }
            },
            [&](const auto&)
            {
                if (locked->drainMarker.has_value())
                {
                    NES_WARNING(
                        "Query {} is already drained by marker {}, ignoring marker {}",
                        localQueryId,
                        locked->drainMarker->getId(),
                        marker.getId());
                    return;
                }
                locked->drainMarker = marker;
                locked->drainEvent = *event;
            }},
        *event);
    return event;
}

std::optional<ReconfigurationMarker> ReconfigurationState::getDrainMarker() const
{
    return internal.rlock()->drainMarker;
}

bool ReconfigurationState::isUpdateAndDrain() const
{
    const auto locked = internal.rlock();
    return locked->drainEvent.has_value() and std::holds_alternative<UpdateAndDrainQueryMetadata>(*locked->drainEvent);
}

ChannelTermination ReconfigurationState::getChannelTermination(const std::string& channelId) const
{
    const auto locked = internal.rlock();
    if (not locked->drainEvent.has_value())
    {
        return ChannelTermination::Close;
    }
    const auto& detachedChannels = std::visit(
        Overloaded{
            [](const DrainQueryMetadata& drain) -> const std::vector<std::string>& { return drain.detachedChannels; },
            [](const UpdateAndDrainQueryMetadata& updateAndDrain) -> const std::vector<std::string>&
            { return updateAndDrain.detachedChannels; },
            [](const UpdateQueryMetadata&) -> const std::vector<std::string>& { std::unreachable(); }},
        *locked->drainEvent);
    return std::ranges::contains(detachedChannels, channelId) ? ChannelTermination::Detach : ChannelTermination::ForwardMarker;
}

void ReconfigurationState::registerSinkUpdateHandler(const std::string& channelId, const void* owner, SinkUpdateHandler handler)
{
    auto locked = internal.wlock();
    auto channel = channelId;
    /// Apply the updates recorded before the sink registered, possibly several in a row.
    for (auto pending = locked->pendingSinkUpdates.find(channel); pending != locked->pendingSinkUpdates.end();
         pending = locked->pendingSinkUpdates.find(channel))
    {
        const auto [update, marker] = std::move(pending->second);
        locked->pendingSinkUpdates.erase(pending);
        channel = handler(update, marker);
    }
    locked->sinkUpdateHandlers.insert_or_assign(channel, std::pair{owner, std::move(handler)});
}

void ReconfigurationState::unregisterSinkUpdateHandlers(const void* owner)
{
    std::erase_if(internal.wlock()->sinkUpdateHandlers, [owner](const auto& entry) { return entry.second.first == owner; });
}

std::ostream& operator<<(std::ostream& os, const ChannelTermination termination)
{
    return os << magic_enum::enum_name(termination);
}

}
