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
#include <stop_token>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>

/// A `BackpressureManager` combines the backpressure votes of all components of a query into one blocked flag per source. Each source
/// waits on its own `BackpressureListener`.
///
/// Every component that wants to influence the sources gets its own `BackpressureVoter`. A voter can cast two kinds of votes:
///
///  - A hard vote blocks all sources. Hard votes are OR-ed: one hard vote is enough to block.
///  - A soft vote applies to the sources of one origin, and has three states: apply, release, or no opinion. Soft votes are combined with
///    a veto: a source is blocked by soft votes only if at least one voter applies and no voter releases.
///
/// So for every source:
///
///     blocked = hardVotes > 0 || (softApplies > 0 && softReleases == 0)
///
/// An operator that needs progress from a source releases it, so no two operators can block each other's slowest source. Hard votes are
/// not subject to the veto, because a full sink must stop all sources, even the ones a window waits for.
///
/// Thread safety: the origin-to-source map and the set of sources are fixed when the manager is created and are read-only afterwards.
/// Only the per-source vote counters change. Each source channel guards its counters with its own lock.
///
/// Lifetime: listeners and voters share the state of the manager, so they may outlive it. Destroying a voter withdraws all its votes, so a
/// source is never blocked by a component that no longer exists.

namespace NES
{

/// Maps every origin of the query plan of this worker to the origins of the sources whose data drives it. Every origin has exactly one
/// producer, so the map is global for the query. A source maps to itself. A network source counts as a source of this worker.
using SourcesOfOrigins = std::unordered_map<OriginId, std::vector<OriginId>>;

namespace detail
{
struct BackpressureChannel;
struct BackpressureState;
}

/// The state of one soft vote of one voter for one origin. A voter starts with `NoOpinion` for every origin.
/// The order is important: when the votes of one voter for several origins meet at the same source, the largest vote wins.
enum class SoftVote : uint8_t
{
    NoOpinion,
    Apply,
    Release
};

/// The handle through which one component votes. A voter is the unit of identity: the manager counts the votes of a voter once, no
/// matter which pipeline or worker thread casts them. So a handler that is shared by several pipelines (e.g., the build and probe
/// pipelines of a join) must create one voter in `start` and use it from all its pipelines.
///
/// Destroying a voter withdraws all its votes. The vote functions only accept an lvalue voter, as a vote of a temporary voter is
/// withdrawn at the end of the statement.
/// A voter is not thread-safe: the caller must serialize all calls on the same voter.
/// Not copyable, as a copy would cast the same votes twice.
class BackpressureVoter
{
public:
    BackpressureVoter(const BackpressureVoter&) = delete;
    BackpressureVoter& operator=(const BackpressureVoter&) = delete;
    BackpressureVoter(BackpressureVoter&&) noexcept;
    BackpressureVoter& operator=(BackpressureVoter&&) noexcept;
    ~BackpressureVoter();

    /// Hard vote for all sources of the query. Return true if the vote of this voter changed.
    bool applyBackpressureToAll() &;
    bool releaseBackpressureToAll() &;

    /// Soft vote for the sources of `origin`, which must be an origin of the query plan of this worker.
    /// Return true if the vote of this voter changed for at least one source.
    bool applyBackpressureTo(OriginId origin) &;
    bool releaseBackpressureTo(OriginId origin) &;
    bool withdrawVote(OriginId origin) &;

private:
    friend class BackpressureManager;
    struct State;
    explicit BackpressureVoter(std::shared_ptr<const detail::BackpressureState> shared);
    bool vote(OriginId origin, SoftVote vote);
    void withdrawAll();

    std::unique_ptr<State> state;
};

/// The source side of one channel. A source waits on its listener before it reads the next buffer.
class BackpressureListener
{
public:
    /// Blocks while the source of this listener is blocked, or until `stopToken` is triggered.
    void wait(const std::stop_token& stopToken) const;

    /// Returns whether the source of this listener is blocked right now.
    [[nodiscard]] bool isBlocked() const;

private:
    friend class BackpressureManager;
    explicit BackpressureListener(std::shared_ptr<detail::BackpressureChannel> channel);
    std::shared_ptr<detail::BackpressureChannel> channel;
};

/// Owns one channel per source of the query, and combines the votes of all voters into one blocked flag per channel.
class BackpressureManager
{
public:
    /// Creates one channel for every source. `sourcesOfOrigins` may only name these sources. Every source maps to itself, even if
    /// `sourcesOfOrigins` does not contain it.
    BackpressureManager(const std::vector<OriginId>& sources, SourcesOfOrigins sourcesOfOrigins);

    /// Not copyable or movable: the running query plan and its pipelines share one manager through a `std::shared_ptr`.
    BackpressureManager(const BackpressureManager&) = delete;
    BackpressureManager& operator=(const BackpressureManager&) = delete;
    BackpressureManager(BackpressureManager&&) = delete;
    BackpressureManager& operator=(BackpressureManager&&) = delete;
    ~BackpressureManager() = default;

    /// Returns the listener of `source`.
    [[nodiscard]] BackpressureListener getListener(OriginId source) const;

    /// Returns a new voter without any votes. Thread-safe.
    [[nodiscard]] BackpressureVoter newVoter() const;

private:
    std::shared_ptr<const detail::BackpressureState> state;
};

/// Creates a manager with a single source and returns a voter and the listener of that source. For tests of a single source or sink.
std::pair<BackpressureVoter, BackpressureListener> createSingleSourceBackpressure();

}
