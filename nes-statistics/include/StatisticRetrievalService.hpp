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
#include <RequestStatisticStatement.hpp>

namespace NES
{

class StatisticCoordinator;

/// Synchronous, one-shot access to a statistic value. This complements (does not replace) the continuous
/// collection path (collectNewStatistic + condition-trigger callbacks): instead of registering a long-running
/// query that fires callbacks, the caller asks for a value and blocks until it is produced.
///
/// retrieveStatistic() drives the full request/response over the existing query machinery:
///   1. starts an ad-hoc build query that populates the StatisticStore,
///   2. starts a probe query that reads the value back out of the store,
///   3. blocks until the value arrives over the coordinator's result pipe, and returns it as a scalar.
///
/// The coordinator's result reader must be running (StatisticCoordinator::startResultReader) before use.
class StatisticRetrievalService
{
public:
    explicit StatisticRetrievalService(StatisticCoordinator& coordinator);

    /// Blocks until the statistic described by `statement` has been built and probed; returns the scalar value,
    /// or std::nullopt on submission failure or timeout.
    [[nodiscard]] std::optional<double> retrieveStatistic(const RequestStatisticBuildStatement& statement) const;

private:
    StatisticCoordinator& coordinator;
};

}
