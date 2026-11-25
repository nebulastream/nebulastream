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
#include <memory>
#include <optional>
#include <stop_token>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>

/// BPSTAT: local benchmark instrumentation, not for upstream. Writes one line per event to $NES_BPSTAT_FILE.
namespace NES::bpstat
{
void event(const char* kind, uint64_t a, uint64_t b = 0, uint64_t c = 0);
}

namespace NES
{

/// What the backpressure manager knows about the origins of the query plan of this worker.
struct OriginMaps
{
    /// Maps every origin to the sources whose data reaches it.
    /// Exactly one operator assigns each origin, so one map applies to the whole query.
    /// A source assigns its own origin, so it maps to itself.
    std::unordered_map<OriginId, std::vector<OriginId>> sourcesOfOrigins;

    /// Maps every origin that an operator assigns, other than a source, to the input origins of that operator.
    /// Today, these operators are the window-based operators.
    /// Origins that are not in the map, e.g., sources, are leaves of the critical path.
    std::unordered_map<OriginId, std::vector<OriginId>> inputsOfOrigins;
};

namespace detail
{
struct SourceGate;
struct BackpressureState;
}

/// The handle through which a component outside the plan of the query votes, e.g., a sink.
/// It casts a hard vote, which blocks all sources of the query.
/// Destroying a voter withdraws its vote.
/// A voter is not thread-safe and not copyable, as a copy would cast the same vote twice.
class ExternalBackpressureVoter
{
public:
    ExternalBackpressureVoter(const ExternalBackpressureVoter&) = delete;
    ExternalBackpressureVoter& operator=(const ExternalBackpressureVoter&) = delete;
    ExternalBackpressureVoter(ExternalBackpressureVoter&& other) noexcept;
    ExternalBackpressureVoter& operator=(ExternalBackpressureVoter&& other) noexcept;
    ~ExternalBackpressureVoter();

    /// Hard vote for all sources of the query.
    /// Returns true if the vote of this voter changed.
    bool applyBackpressureToAll() &;
    bool releaseBackpressureToAll() &;

private:
    friend class BackpressureManager;
    explicit ExternalBackpressureVoter(std::shared_ptr<const detail::BackpressureState> shared);

    /// Null after a move.
    std::shared_ptr<const detail::BackpressureState> shared;
    bool applied = false;
};

/// The handle through which an operator inside the plan of the query votes, e.g., a window-based operator.
/// It casts soft votes on the input origins of the operator, and it knows the output origin of the operator.
/// The manager counts the votes of a voter once, no matter which pipeline or worker thread casts them.
/// So a handler that several pipelines share, e.g., the build and probe pipelines of a join, creates one voter and uses it from all.
/// Destroying a voter withdraws all its votes.
/// A voter is not thread-safe and not copyable, as a copy would cast the same votes twice.
class InternalBackpressureVoter
{
public:
    InternalBackpressureVoter(const InternalBackpressureVoter&) = delete;
    InternalBackpressureVoter& operator=(const InternalBackpressureVoter&) = delete;
    InternalBackpressureVoter(InternalBackpressureVoter&&) noexcept;
    InternalBackpressureVoter& operator=(InternalBackpressureVoter&&) noexcept;
    ~InternalBackpressureVoter();

    /// Soft vote for the sources of `origin`, which must be an origin of the query plan of this worker.
    /// A later vote on the same origin replaces the earlier one.
    /// Returns true if the vote of this voter changed for at least one source.
    bool applyBackpressureTo(OriginId origin) &;
    bool releaseBackpressureTo(OriginId origin) &;

private:
    friend class BackpressureManager;
    struct State;
    InternalBackpressureVoter(std::shared_ptr<const detail::BackpressureState> shared, OriginId outputOrigin);
    bool vote(OriginId origin, bool apply);
    void withdrawAll();

    std::unique_ptr<State> state;
};

/// A source waits on its listener before it reads the next buffer.
class BackpressureListener
{
public:
    /// Blocks while the source of this listener is blocked, or until `stopToken` is triggered.
    void wait(const std::stop_token& stopToken) const;

    [[nodiscard]] bool isBlocked() const;

private:
    friend class BackpressureManager;
    explicit BackpressureListener(std::shared_ptr<detail::SourceGate> gate);
    std::shared_ptr<detail::SourceGate> gate;
};

/// Combines the votes of all voters of one running query into one blocked flag per source.
///
/// An external voter casts hard votes for pressure from outside the query.
/// A sink casts a hard vote when its downstream cannot accept more data.
/// A hard vote blocks all sources.
///
/// An internal voter casts soft votes for pressure from inside the plan.
/// It belongs to an operator that builds state over several input origins, e.g., a window-based operator.
/// A soft vote applies to or releases the sources of one origin.
/// The operator applies to input origins that run too far ahead and releases the others.
/// A release states that the operator waits on that input origin.
/// The manager follows these waits from the top of the plan down to the sources.
/// This gives the critical path: the sources that the slowest chain of operators waits on.
/// An operator without a release, e.g., one that has not started yet, waits on all its inputs.
///
///     blocked = hardVotes > 0 || (softApplies > 0 && !critical)
///
/// So an operator above can throttle the sources of an operator below while it does not wait on that operator.
///
/// Soft votes never block a source on the critical path, so it advances.
/// Then the slowest input of the top operator advances, the gaps close, and the operators withdraw their applies.
/// This is why soft votes cannot deadlock.
/// A full sink must stop all sources, so hard votes also block critical sources.
///
/// Listeners and voters share the state of the manager, so they may outlive it.
class BackpressureManager
{
public:
    /// `origins.sourcesOfOrigins` may only name `sources`.
    /// If `origins.inputsOfOrigins` is empty, e.g., for a plan without windows, no source is critical.
    BackpressureManager(const std::vector<OriginId>& sources, OriginMaps origins);

    /// Not copyable or movable: the running query plan and its pipelines share one manager through a `std::shared_ptr`.
    BackpressureManager(const BackpressureManager&) = delete;
    BackpressureManager& operator=(const BackpressureManager&) = delete;
    BackpressureManager(BackpressureManager&&) = delete;
    BackpressureManager& operator=(BackpressureManager&&) = delete;
    ~BackpressureManager() = default;

    [[nodiscard]] BackpressureListener getListener(OriginId source) const;

    /// Returns a new external voter without a vote.
    /// Thread-safe.
    [[nodiscard]] ExternalBackpressureVoter newExternalVoter() const;

    /// Returns a new internal voter without any votes, for the operator that produces `outputOrigin`.
    /// Thread-safe.
    [[nodiscard]] InternalBackpressureVoter newInternalVoter(OriginId outputOrigin) const;

private:
    std::shared_ptr<const detail::BackpressureState> state;
};

}
