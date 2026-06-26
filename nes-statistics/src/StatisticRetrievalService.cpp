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

#include <StatisticRetrievalService.hpp>

#include <optional>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <RequestStatisticStatement.hpp>
#include <StatisticCoordinator.hpp>
#include <StatisticRegistry.hpp>

namespace NES
{

StatisticRetrievalService::StatisticRetrievalService(StatisticCoordinator& coordinator) : coordinator(coordinator)
{
}

std::optional<double> StatisticRetrievalService::retrieveStatistic(const RequestStatisticBuildStatement& statement) const
{
    /// 1. Start the ad-hoc build query (deduplicated by the coordinator); it populates the StatisticStore.
    const auto collectResult = coordinator.collectNewStatistic(statement);
    if (not collectResult.has_value())
    {
        NES_WARNING("StatisticRetrievalService: failed to start build query: {}", collectResult.error().what());
        return std::nullopt;
    }

    /// 2. + 3. Start a probe query for the same key and block until its value travels back over the pipe.
    /// The build query writes (statisticId, 0, windowSizeMs, value), so the probe queries that exact window.
    const StatisticRegistry::Key key{
        .metric = statement.metric,
        .collectionDomain = statement.domain,
        .windowSize = Windowing::TimeMeasure{statement.windowSizeMs}};

    const auto value = coordinator.getStatistics(key, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{statement.windowSizeMs});

    /// 4. Tear down the ad-hoc build query (blocking) before returning, so it does not keep running after the
    /// one-shot retrieval. A fresh retrieval with the same request will start a new build.
    coordinator.stopStatistic(key);

    return value;
}

}
