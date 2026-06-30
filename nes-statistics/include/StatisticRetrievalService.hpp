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
#include <string>

namespace NES
{

class StatisticCoordinator;

/// Synchronous, ad-hoc access to a statistic value. Statistics are identified by the logical source they are
/// collected on (their collection domain). The model is "continuous build, ad-hoc probe":
///   * deployStatisticBuild(source) spins up a long-running (mock) build query collecting the statistic for
///     `source`. Builds are deduplicated by source, so collecting on N distinct sources yields N build queries,
///     while re-requesting the same source reuses the running one. In the PoC this is called once per query
///     carrying `GET_STATISTICS=true`, keyed by that query's own source.
///   * retrieveStatistic(source) probes the build collecting `source`'s statistic and returns its current value.
///     It does NOT build or tear anything down; it relies on a previously deployed build.
///
/// The coordinator's result reader must be running (StatisticCoordinator::startResultReader) before use.
class StatisticRetrievalService
{
public:
    explicit StatisticRetrievalService(StatisticCoordinator& coordinator);

    /// Deploys (or reuses) the (mock) build query that collects the statistic for `source`. The caller decides
    /// WHETHER to collect (e.g. only for queries carrying GET_STATISTICS). `source` should be canonicalized.
    void deployStatisticBuild(const std::string& source) const;

    /// Probes the build collecting `source`'s statistic (ad-hoc) and returns its current value, or std::nullopt if
    /// no build for `source` is running or the probe times out. Never deploys or tears down a build.
    [[nodiscard]] std::optional<double> retrieveStatistic(const std::string& source) const;

private:
    StatisticCoordinator& coordinator;
};

}
