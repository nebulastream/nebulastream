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
#include <cstddef>
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

#include <chrono>
#include <cstdio>
#include <cstdlib>

namespace NES
{

/// BPSTAT: local benchmark instrumentation, not for upstream.
void bpstat::event(const char* kind, const uint64_t a, const uint64_t b, const uint64_t c)
{
    static FILE* const file = []() -> FILE*
    {
        const char* path = std::getenv("NES_BPSTAT_FILE");
        if (path == nullptr)
        {
            return nullptr;
        }
        FILE* opened = std::fopen(path, "w");
        if (opened != nullptr)
        {
            std::setvbuf(opened, nullptr, _IOLBF, 1 << 16);
        }
        return opened;
    }();
    if (file == nullptr)
    {
        return;
    }
    static std::mutex mutex;
    const auto now = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    const std::scoped_lock lock(mutex);
    std::fprintf(file, "%lld %s %llu %llu %llu\n", static_cast<long long>(now), kind, static_cast<unsigned long long>(a), static_cast<unsigned long long>(b), static_cast<unsigned long long>(c));
}

namespace detail
{
struct SourceGate
{
    struct Votes
    {
        uint64_t hard = 0;
        uint64_t softApply = 0;
        bool critical = false;

        [[nodiscard]] bool blocked() const { return hard > 0 || (softApply > 0 && !critical); }
    };

    folly::Synchronized<Votes, std::mutex> votes;
    std::condition_variable_any unblocked;
    uint64_t id = 0;

    /// Applies `change` to the votes, and wakes the listener if the source is no longer blocked.
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

    void addApply(const int64_t delta)
    {
        update(
            [&](Votes& current)
            {
                INVARIANT(delta > 0 || current.softApply > 0, "A voter withdraws a soft apply it has not cast");
                current.softApply += delta;
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
    std::unordered_map<OriginId, std::shared_ptr<SourceGate>> gates;
    std::unordered_map<OriginId, std::vector<OriginId>> sourcesOfOrigins;
    std::unordered_map<OriginId, std::vector<OriginId>> inputsOfOrigins;
    /// One more than the largest origin of the query plan.
    /// The optimizer numbers the origins of a query from 1, so vectors indexed by the raw origin stay small.
    size_t originSlots = 0;
    /// The origins in `inputsOfOrigins` that no operator in `inputsOfOrigins` consumes, where the critical path starts.
    std::vector<OriginId> topOrigins;

    /// For every output origin, how many internal voters of its operator wait on each input origin.
    using Waits = std::unordered_map<OriginId, std::unordered_map<OriginId, uint64_t>>;

    struct CriticalPath
    {
        Waits waits;
        /// Scratch space of the walk, indexed by the raw origin and kept so that a walk does not allocate while it has the lock.
        std::vector<bool> visited;
        /// After a walk, whether each source is on the critical path.
        std::vector<bool> critical;
        std::vector<OriginId> pending;
    };

    /// A change of the waits keeps this lock while it writes the result of its walk to the gates.
    /// So the critical flags always match the latest waits, and concurrent changes apply their results in order.
    /// This lock is taken before the lock of any gate, never after.
    mutable folly::Synchronized<CriticalPath, std::mutex> path;

    /// Marks the sources on the critical path in `current.critical`.
    /// An operator without waits waits on all its inputs.
    void walk(CriticalPath& current) const
    {
        auto& [waits, visited, critical, pending] = current;
        visited.assign(originSlots, false);
        critical.assign(originSlots, false);
        pending.assign(topOrigins.begin(), topOrigins.end());
        while (!pending.empty())
        {
            const auto origin = pending.back();
            pending.pop_back();
            if (visited[origin.getRawValue()])
            {
                continue;
            }
            visited[origin.getRawValue()] = true;
            const auto inputs = inputsOfOrigins.find(origin);
            if (inputs == inputsOfOrigins.end())
            {
                /// No operator of this plan assigns the origin, so a source delivers it.
                if (const auto sources = sourcesOfOrigins.find(origin); sources != sourcesOfOrigins.end())
                {
                    for (const auto source : sources->second)
                    {
                        critical[source.getRawValue()] = true;
                    }
                }
                continue;
            }
            if (const auto waiting = waits.find(origin); waiting != waits.end())
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
    }

    /// Sets the critical flag of every source from a walk over the waits of `current`.
    /// Requires the lock of `path`, so concurrent changes apply their results in order.
    /// A gate wakes its listener only if the flag unblocks the source, so a flag that does not change has no effect.
    void updateCritical(CriticalPath& current) const
    {
        walk(current);
        for (const auto& [source, gate] : gates)
        {
            gate->setCritical(current.critical[source.getRawValue()]);
        }
    }

    void changeWait(const OriginId output, const OriginId input, const int64_t delta) const
    {
        auto locked = path.lock();
        auto& count = locked->waits[output][input];
        INVARIANT(delta > 0 || count > 0, "An internal voter withdraws a wait it has not cast");
        count += delta;
        if (count == 0)
        {
            locked->waits.at(output).erase(input);
            if (locked->waits.at(output).empty())
            {
                locked->waits.erase(output);
            }
        }
        updateCritical(*locked);
    }
};
}

ExternalBackpressureVoter::ExternalBackpressureVoter(std::shared_ptr<const detail::BackpressureState> shared) : shared(std::move(shared))
{
}

ExternalBackpressureVoter::ExternalBackpressureVoter(ExternalBackpressureVoter&& other) noexcept
    : shared(std::move(other.shared)), applied(std::exchange(other.applied, false))
{
}

/// NOLINTNEXTLINE(bugprone-exception-escape): withdrawing the vote only throws on an invariant violation or a failed mutex lock.
ExternalBackpressureVoter& ExternalBackpressureVoter::operator=(ExternalBackpressureVoter&& other) noexcept
{
    if (this != &other)
    {
        releaseBackpressureToAll();
        shared = std::move(other.shared);
        applied = std::exchange(other.applied, false);
    }
    return *this;
}

/// NOLINTNEXTLINE(bugprone-exception-escape): withdrawing the vote only throws on an invariant violation or a failed mutex lock.
ExternalBackpressureVoter::~ExternalBackpressureVoter()
{
    releaseBackpressureToAll();
}

bool ExternalBackpressureVoter::applyBackpressureToAll() &
{
    if (std::exchange(applied, true))
    {
        return false;
    }
    bpstat::event("hard_apply", 0);
    for (const auto& gate : shared->gates | std::views::values)
    {
        gate->addHard(1);
    }
    return true;
}

bool ExternalBackpressureVoter::releaseBackpressureToAll() &
{
    /// A moved-from voter has no vote, so it does not use its state.
    if (!std::exchange(applied, false))
    {
        return false;
    }
    bpstat::event("hard_release", 0);
    for (const auto& gate : shared->gates | std::views::values)
    {
        gate->addHard(-1);
    }
    return true;
}

struct InternalBackpressureVoter::State
{
    std::shared_ptr<const detail::BackpressureState> shared;
    /// The origin that the operator of this voter produces.
    OriginId outputOrigin;
    /// The soft vote on each origin, indexed by the raw origin: true for an apply, false for a release.
    std::vector<std::optional<bool>> originVotes;
    /// For every source, indexed by the raw origin, how many applied origins in `originVotes` reach it.
    /// The voter applies to a source while its count is above 0.
    std::vector<uint64_t> appliesPerSource;
};

InternalBackpressureVoter::InternalBackpressureVoter(std::shared_ptr<const detail::BackpressureState> shared, const OriginId outputOrigin)
    : state(
          std::make_unique<State>(State{
              .shared = shared,
              .outputOrigin = outputOrigin,
              .originVotes = std::vector<std::optional<bool>>(shared->originSlots),
              .appliesPerSource = std::vector<uint64_t>(shared->originSlots)}))
{
}

InternalBackpressureVoter::InternalBackpressureVoter(InternalBackpressureVoter&&) noexcept = default;

/// NOLINTNEXTLINE(bugprone-exception-escape): withdrawing votes only throws on an invariant violation or a failed mutex lock.
InternalBackpressureVoter& InternalBackpressureVoter::operator=(InternalBackpressureVoter&& other) noexcept
{
    if (this != &other)
    {
        withdrawAll();
        state = std::move(other.state);
    }
    return *this;
}

/// NOLINTNEXTLINE(bugprone-exception-escape): withdrawing votes only throws on an invariant violation or a failed mutex lock.
InternalBackpressureVoter::~InternalBackpressureVoter()
{
    withdrawAll();
}

void InternalBackpressureVoter::withdrawAll()
{
    /// A moved-from voter has no state and no votes.
    if (!state)
    {
        return;
    }
    for (size_t index = 0; index < state->shared->originSlots; ++index)
    {
        if (state->appliesPerSource[index] > 0)
        {
            state->shared->gates.at(OriginId(index))->addApply(-1);
        }
        if (state->originVotes[index] == false)
        {
            state->shared->changeWait(state->outputOrigin, OriginId(index), -1);
        }
    }
    std::ranges::fill(state->appliesPerSource, 0);
    std::ranges::fill(state->originVotes, std::nullopt);
}

bool InternalBackpressureVoter::applyBackpressureTo(const OriginId origin) &
{
    return vote(origin, true);
}

bool InternalBackpressureVoter::releaseBackpressureTo(const OriginId origin) &
{
    return vote(origin, false);
}

bool InternalBackpressureVoter::vote(const OriginId origin, const bool apply)
{
    const auto sourcesOfOrigin = state->shared->sourcesOfOrigins.find(origin);
    INVARIANT(sourcesOfOrigin != state->shared->sourcesOfOrigins.end(), "Origin {} is not an origin of this query", origin);

    auto& originVote = state->originVotes[origin.getRawValue()];
    const bool hadVote = originVote.has_value();
    const bool wasApplied = originVote == true;
    if (originVote == apply)
    {
        return false;
    }
    originVote = apply;

    /// A release is a wait on the critical path. Only the critical path keeps the sources of a waited origin unblocked.
    if (!apply || hadVote)
    {
        state->shared->changeWait(state->outputOrigin, origin, apply ? -1 : 1);
    }
    if (apply == wasApplied)
    {
        return false;
    }

    bool changed = false;
    for (const auto source : sourcesOfOrigin->second)
    {
        auto& count = state->appliesPerSource[source.getRawValue()];
        count = apply ? count + 1 : count - 1;
        if (count == (apply ? 1 : 0))
        {
            state->shared->gates.at(source)->addApply(apply ? 1 : -1);
            changed = true;
        }
    }
    return changed;
}

BackpressureListener::BackpressureListener(std::shared_ptr<detail::SourceGate> gate) : gate(std::move(gate))
{
}

void BackpressureListener::wait(const std::stop_token& stopToken) const
{
    auto votes = gate->votes.lock();
    /// A source calls this before every buffer, and the wait with a stop token registers a stop callback even if it does not block.
    if (!votes->blocked())
    {
        return;
    }
    const auto begin = std::chrono::steady_clock::now();
    bpstat::event("block", gate->id, votes->hard, votes->softApply);
    gate->unblocked.wait(votes.as_lock(), stopToken, [&] { return !votes->blocked(); });
    const auto blockedFor = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin).count();
    bpstat::event("unblock", gate->id, static_cast<uint64_t>(blockedFor));
}

bool BackpressureListener::isBlocked() const
{
    return gate->votes.lock()->blocked();
}

BackpressureManager::BackpressureManager(const std::vector<OriginId>& sources, OriginMaps origins)
{
    auto& [sourcesOfOrigins, inputsOfOrigins] = origins;
    auto shared = std::make_shared<detail::BackpressureState>();
    for (const auto source : sources)
    {
        shared->gates.try_emplace(source, std::make_shared<detail::SourceGate>());
        shared->gates.at(source)->id = source.getRawValue();
        sourcesOfOrigins.try_emplace(source, std::vector{source});
    }
    for (const auto& [origin, sourcesOfOrigin] : sourcesOfOrigins)
    {
        for (const auto source : sourcesOfOrigin)
        {
            INVARIANT(shared->gates.contains(source), "Source {} of origin {} is not a source of this query", source, origin);
        }
        shared->originSlots = std::max(shared->originSlots, origin.getRawValue() + 1);
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
    for (const auto& [origin, inputs] : inputsOfOrigins)
    {
        INVARIANT(shared->sourcesOfOrigins.contains(origin), "Output origin {} has no sources", origin);
        for (const auto input : inputs)
        {
            INVARIANT(shared->sourcesOfOrigins.contains(input), "Input origin {} of origin {} has no sources", input, origin);
        }
    }
    shared->inputsOfOrigins = std::move(inputsOfOrigins);
    shared->updateCritical(*shared->path.lock());
    state = std::move(shared);
}

BackpressureListener BackpressureManager::getListener(const OriginId source) const
{
    const auto gate = state->gates.find(source);
    INVARIANT(gate != state->gates.end(), "{} is not a source of this query", source);
    return BackpressureListener{gate->second};
}

ExternalBackpressureVoter BackpressureManager::newExternalVoter() const
{
    return ExternalBackpressureVoter{state};
}

InternalBackpressureVoter BackpressureManager::newInternalVoter(const OriginId outputOrigin) const
{
    return InternalBackpressureVoter{state, outputOrigin};
}

}
