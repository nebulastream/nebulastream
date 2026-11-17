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

#include <BackpressureManager.hpp>

#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ranges>
#include <stop_token>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// The vote of one voter at one origin or source. The order is important: when the votes of one voter for several origins meet at the
/// same source, the largest vote wins.
enum class SoftVote : uint8_t
{
    NoOpinion,
    Apply,
    Release
};
}

namespace detail
{
struct BackpressureChannel
{
    struct Votes
    {
        uint64_t hard = 0;
        uint64_t softApply = 0;
        uint64_t softRelease = 0;

        [[nodiscard]] bool blocked() const { return hard > 0 || (softApply > 0 && softRelease == 0); }
    };

    folly::Synchronized<Votes, std::mutex> votes;
    std::condition_variable_any unblocked;

    /// `change` adds or removes the votes of one voter. Removing a vote the voter has not cast is an invariant violation.
    template <typename Change>
    void update(Change change)
    {
        bool wakeListener = false;
        {
            auto locked = votes.lock();
            const auto wasBlocked = locked->blocked();
            change(*locked);
            wakeListener = wasBlocked && !locked->blocked();
        }
        if (wakeListener)
        {
            unblocked.notify_all();
        }
    }

    void addSoft(const SoftVote vote, const int64_t delta)
    {
        if (vote == SoftVote::NoOpinion)
        {
            return;
        }
        update(
            [&](Votes& current)
            {
                auto& counter = vote == SoftVote::Apply ? current.softApply : current.softRelease;
                INVARIANT(delta > 0 || counter > 0, "A voter withdraws a soft vote it has not cast");
                counter += delta;
            });
    }

    void addHard(const int64_t delta)
    {
        update(
            [&](Votes& current)
            {
                INVARIANT(delta > 0 || current.hard > 0, "A voter withdraws a hard vote it has not cast");
                current.hard += delta;
            });
    }
};

struct BackpressureState
{
    std::unordered_map<OriginId, std::shared_ptr<BackpressureChannel>> channels;
    SourcesOfOrigins sourcesOfOrigins;
};
}

struct BackpressureVoter::State
{
    std::shared_ptr<const detail::BackpressureState> shared;
    bool hard = false;
    /// The vote of this voter for every origin it has an opinion on.
    std::unordered_map<OriginId, SoftVote> originVotes;
    /// The vote this voter has counted at every source. Derived from `originVotes`: the largest vote of all origins that the source drives.
    std::unordered_map<OriginId, SoftVote> sourceVotes;
};

BackpressureVoter::BackpressureVoter(std::shared_ptr<const detail::BackpressureState> shared)
    : state(std::make_unique<State>(State{.shared = std::move(shared), .hard = false, .originVotes = {}, .sourceVotes = {}}))
{
}

BackpressureVoter::BackpressureVoter(BackpressureVoter&&) noexcept = default;

/// NOLINTNEXTLINE(bugprone-exception-escape): withdrawing votes only throws on an invariant violation or a failed mutex lock.
BackpressureVoter& BackpressureVoter::operator=(BackpressureVoter&& other) noexcept
{
    if (this != &other)
    {
        withdrawAll();
        state = std::move(other.state);
    }
    return *this;
}

/// NOLINTNEXTLINE(bugprone-exception-escape): withdrawing votes only throws on an invariant violation or a failed mutex lock.
BackpressureVoter::~BackpressureVoter()
{
    withdrawAll();
}

void BackpressureVoter::withdrawAll()
{
    /// A moved-from voter has no state and no votes.
    if (!state)
    {
        return;
    }
    releaseBackpressureToAll();
    for (const auto& [source, vote] : state->sourceVotes)
    {
        state->shared->channels.at(source)->addSoft(vote, -1);
    }
    state->sourceVotes.clear();
    state->originVotes.clear();
}

bool BackpressureVoter::applyBackpressureToAll() &
{
    if (std::exchange(state->hard, true))
    {
        return false;
    }
    for (const auto& channel : state->shared->channels | std::views::values)
    {
        channel->addHard(1);
    }
    return true;
}

bool BackpressureVoter::releaseBackpressureToAll() &
{
    if (!std::exchange(state->hard, false))
    {
        return false;
    }
    for (const auto& channel : state->shared->channels | std::views::values)
    {
        channel->addHard(-1);
    }
    return true;
}

bool BackpressureVoter::applyBackpressureTo(const OriginId origin) &
{
    return vote(origin, true);
}

bool BackpressureVoter::releaseBackpressureTo(const OriginId origin) &
{
    return vote(origin, false);
}

bool BackpressureVoter::vote(const OriginId origin, const bool apply)
{
    const auto vote = apply ? SoftVote::Apply : SoftVote::Release;
    const auto& sourcesOfOrigins = state->shared->sourcesOfOrigins;
    const auto sourcesOfOrigin = sourcesOfOrigins.find(origin);
    INVARIANT(sourcesOfOrigin != sourcesOfOrigins.end(), "Origin {} is not an origin of this query", origin);

    state->originVotes.insert_or_assign(origin, vote);

    bool changed = false;
    for (const auto source : sourcesOfOrigin->second)
    {
        auto newVote = SoftVote::NoOpinion;
        for (const auto& [votedOrigin, originVote] : state->originVotes)
        {
            if (std::ranges::contains(sourcesOfOrigins.at(votedOrigin), source))
            {
                newVote = std::max(newVote, originVote);
            }
        }
        const auto oldVote = state->sourceVotes.contains(source) ? state->sourceVotes.at(source) : SoftVote::NoOpinion;
        if (newVote == oldVote)
        {
            continue;
        }
        const auto& channel = state->shared->channels.at(source);
        /// Adding the new vote before removing the old one keeps a released source from appearing blocked in between.
        channel->addSoft(newVote, 1);
        channel->addSoft(oldVote, -1);
        if (newVote == SoftVote::NoOpinion)
        {
            state->sourceVotes.erase(source);
        }
        else
        {
            state->sourceVotes.insert_or_assign(source, newVote);
        }
        changed = true;
    }
    return changed;
}

BackpressureListener::BackpressureListener(std::shared_ptr<detail::BackpressureChannel> channel) : channel(std::move(channel))
{
}

void BackpressureListener::wait(const std::stop_token& stopToken) const
{
    auto votes = channel->votes.lock();
    channel->unblocked.wait(votes.as_lock(), stopToken, [&] { return !votes->blocked(); });
}

bool BackpressureListener::isBlocked() const
{
    return channel->votes.lock()->blocked();
}

BackpressureManager::BackpressureManager(const std::vector<OriginId>& sources, SourcesOfOrigins sourcesOfOrigins)
{
    auto shared = std::make_shared<detail::BackpressureState>();
    for (const auto source : sources)
    {
        shared->channels.try_emplace(source, std::make_shared<detail::BackpressureChannel>());
        sourcesOfOrigins.try_emplace(source, std::vector{source});
    }
    for (const auto& [origin, sourcesOfOrigin] : sourcesOfOrigins)
    {
        for (const auto source : sourcesOfOrigin)
        {
            INVARIANT(shared->channels.contains(source), "Origin {} is driven by {}, which is not a source of this query", origin, source);
        }
    }
    shared->sourcesOfOrigins = std::move(sourcesOfOrigins);
    state = std::move(shared);
}

BackpressureListener BackpressureManager::getListener(const OriginId source) const
{
    const auto channel = state->channels.find(source);
    INVARIANT(channel != state->channels.end(), "{} is not a source of this query", source);
    return BackpressureListener{channel->second};
}

BackpressureVoter BackpressureManager::newVoter() const
{
    return BackpressureVoter{state};
}

std::pair<BackpressureVoter, BackpressureListener> createSingleSourceBackpressure()
{
    const auto source = INITIAL<OriginId>;
    const BackpressureManager manager({source}, {});
    return {manager.newVoter(), manager.getListener(source)};
}

}
