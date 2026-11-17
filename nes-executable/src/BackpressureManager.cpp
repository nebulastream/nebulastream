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
#include <optional>
#include <ranges>
#include <stop_token>
#include <unordered_map>
#include <unordered_set>
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
/// When the votes of one voter for several origins meet at the same source, the largest vote wins.
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
        bool critical = false;

        [[nodiscard]] bool blocked() const { return hard > 0 || (softApply > 0 && softRelease == 0 && !critical); }
    };

    folly::Synchronized<Votes, std::mutex> votes;
    std::condition_variable_any unblocked;

    /// `change` adds or removes the votes of one voter.
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

    void setCritical(const bool critical)
    {
        update([&](Votes& current) { current.critical = critical; });
    }
};

struct BackpressureState
{
    std::unordered_map<OriginId, std::shared_ptr<BackpressureChannel>> channels;
    SourcesOfOrigins sourcesOfOrigins;
    InputsOfOrigins inputsOfOrigins;
    /// Origins that no window of this worker consumes, where the critical path starts.
    std::vector<OriginId> topOrigins;

    /// For every window output origin, how many voters of that window wait on each input origin.
    /// Its lock is taken before the lock of any channel, never after.
    using Waits = std::unordered_map<OriginId, std::unordered_map<OriginId, uint64_t>>;
    mutable folly::Synchronized<Waits, std::mutex> waits;

    /// A window without waits waits on all its inputs.
    [[nodiscard]] std::unordered_set<OriginId> criticalSources(const Waits& current) const
    {
        std::unordered_set<OriginId> visited;
        std::unordered_set<OriginId> critical;
        std::vector<OriginId> pending = topOrigins;
        while (!pending.empty())
        {
            const auto origin = pending.back();
            pending.pop_back();
            if (!visited.insert(origin).second)
            {
                continue;
            }
            const auto inputs = inputsOfOrigins.find(origin);
            if (inputs == inputsOfOrigins.end())
            {
                /// A source, or an origin of an upstream worker that reaches this worker through a network source.
                if (const auto sources = sourcesOfOrigins.find(origin); sources != sourcesOfOrigins.end())
                {
                    critical.insert(sources->second.begin(), sources->second.end());
                }
                continue;
            }
            if (const auto waiting = current.find(origin); waiting != current.end())
            {
                for (const auto input : waiting->second | std::views::keys)
                {
                    pending.push_back(input);
                }
            }
            else
            {
                pending.insert(pending.end(), inputs->second.begin(), inputs->second.end());
            }
        }
        return critical;
    }

    /// Recomputes the critical flags while holding the lock of the waits, so concurrent changes apply their results in order.
    void updateCritical(const Waits& current) const
    {
        const auto critical = criticalSources(current);
        for (const auto& [source, channel] : channels)
        {
            channel->setCritical(critical.contains(source));
        }
    }

    void changeWait(const OriginId output, const OriginId input, const int64_t delta) const
    {
        auto locked = waits.lock();
        auto& count = (*locked)[output][input];
        INVARIANT(delta > 0 || count > 0, "A window withdraws a wait it has not cast");
        count += delta;
        if (count == 0)
        {
            locked->at(output).erase(input);
            if (locked->at(output).empty())
            {
                locked->erase(output);
            }
        }
        updateCritical(*locked);
    }
};
}

struct BackpressureVoter::State
{
    std::shared_ptr<const detail::BackpressureState> shared;
    /// Set for the voter of a window-based operator, whose releases are waits on input origins, not vetoes.
    std::optional<OriginId> outputOrigin;
    bool hard = false;
    std::unordered_map<OriginId, SoftVote> originVotes;
    /// For every source, the largest vote in `originVotes` among the origins that the source reaches.
    std::unordered_map<OriginId, SoftVote> sourceVotes;
};

BackpressureVoter::BackpressureVoter(std::shared_ptr<const detail::BackpressureState> shared, const std::optional<OriginId> outputOrigin)
    : state(
          std::make_unique<State>(
              State{.shared = std::move(shared), .outputOrigin = outputOrigin, .hard = false, .originVotes = {}, .sourceVotes = {}}))
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
    if (state->outputOrigin)
    {
        for (const auto& [origin, vote] : state->originVotes)
        {
            if (vote == SoftVote::Release)
            {
                state->shared->changeWait(*state->outputOrigin, origin, -1);
            }
        }
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

    const auto previous = state->originVotes.contains(origin) ? state->originVotes.at(origin) : SoftVote::NoOpinion;
    if (state->outputOrigin && (previous == SoftVote::Release) != (vote == SoftVote::Release))
    {
        state->shared->changeWait(*state->outputOrigin, origin, vote == SoftVote::Release ? 1 : -1);
    }

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
        /// The critical path accounts for the release of a window.
        /// At the source, the release only cancels the window's own apply.
        if (state->outputOrigin && newVote == SoftVote::Release)
        {
            newVote = SoftVote::NoOpinion;
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

BackpressureManager::BackpressureManager(
    const std::vector<OriginId>& sources, SourcesOfOrigins sourcesOfOrigins, InputsOfOrigins inputsOfOrigins)
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
    std::unordered_set<OriginId> consumed;
    for (const auto& inputs : inputsOfOrigins | std::views::values)
    {
        consumed.insert(inputs.begin(), inputs.end());
    }
    for (const auto origin : inputsOfOrigins | std::views::keys)
    {
        if (!consumed.contains(origin))
        {
            shared->topOrigins.push_back(origin);
        }
    }
    shared->inputsOfOrigins = std::move(inputsOfOrigins);
    shared->updateCritical(*shared->waits.lock());
    state = std::move(shared);
}

BackpressureManager::BackpressureManager(const std::vector<OriginId>& sources, SourcesOfOrigins sourcesOfOrigins)
    : BackpressureManager(sources, std::move(sourcesOfOrigins), {})
{
}

BackpressureListener BackpressureManager::getListener(const OriginId source) const
{
    const auto channel = state->channels.find(source);
    INVARIANT(channel != state->channels.end(), "{} is not a source of this query", source);
    return BackpressureListener{channel->second};
}

BackpressureVoter BackpressureManager::newVoter() const
{
    return BackpressureVoter{state, std::nullopt};
}

BackpressureVoter BackpressureManager::newWindowVoter(const OriginId outputOrigin) const
{
    return BackpressureVoter{state, outputOrigin};
}

std::pair<BackpressureVoter, BackpressureListener> createSingleSourceBackpressure()
{
    const auto source = INITIAL<OriginId>;
    const BackpressureManager manager({source}, {});
    return {manager.newVoter(), manager.getListener(source)};
}

}
