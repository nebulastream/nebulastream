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

#include <optional>

namespace NES
{

class StatisticCoordinator;

/// Synchronous, ad-hoc access to a statistic value. The model is "continuous build, ad-hoc probe":
///   * deployStatisticBuildIfAbsent() spins up a long-running (mock) build query that keeps the StatisticStore
///     populated. It is idempotent (the coordinator deduplicates), so the first call deploys the build and later
///     calls are cheap no-ops. The decision of WHETHER to collect a statistic is the caller's: in the PoC this is
///     only called for a query carrying `GET_STATISTICS=true`, simulating how a statistic would be collected
///     alongside that query.
///   * retrieveStatistic() probes that already-running build for its current value and returns it as a scalar. It
///     does NOT build or tear anything down; it relies on a previously deployed build.
///
/// The coordinator's result reader must be running (StatisticCoordinator::startResultReader) before use.
class StatisticRetrievalService
{
public:
    explicit StatisticRetrievalService(StatisticCoordinator& coordinator);

    /// Deploys the (mock) statistic build query unless one is already running. Idempotent: deduplicated by the
    /// coordinator, so only the first call actually deploys the continuous build. This method always deploys when
    /// called — the caller decides whether collection was requested (e.g. via GET_STATISTICS).
    void deployStatisticBuildIfAbsent() const;

    /// Probes the already-running build (ad-hoc) and returns its current value, or std::nullopt if no build is
    /// running yet or the probe times out. Never deploys or tears down a build.
    [[nodiscard]] std::optional<double> retrieveStatistic() const;

private:
    StatisticCoordinator& coordinator;
};

}
