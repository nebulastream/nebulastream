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

#include <cstdint>
#include <optional>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <CollectionDomain.hpp>
#include <ErrorHandling.hpp>
#include <Metric.hpp>
#include <RequestStatisticStatement.hpp>
#include <StatisticCoordinator.hpp>
#include <StatisticRegistry.hpp>

namespace NES
{

namespace
{
/// The single mock statistic this PoC collects. Both the build (deployment) and the probe (retrieval) refer to the
/// same metric/domain/window so the probe finds the build the coordinator registered under this key. In a real
/// system this would be derived from the query being optimized; here it is a fixed stand-in.
constexpr uint64_t MOCK_WINDOW_SIZE_MS = 1000;

RequestStatisticBuildStatement mockBuildRequest()
{
    return RequestStatisticBuildStatement{
        .domain = DataDomain{.logicalSourceName = "optimizerSource", .fieldName = "value"},
        .metric = Metric::Average,
        .windowSizeMs = MOCK_WINDOW_SIZE_MS,
        .windowAdvanceMs = std::nullopt,
        .eventTimeFieldName = std::nullopt,
        .conditionTrigger = std::nullopt,
        .options = {}};
}

StatisticRegistry::Key mockKey()
{
    const auto request = mockBuildRequest();
    return StatisticRegistry::Key{
        .metric = request.metric, .collectionDomain = request.domain, .windowSize = Windowing::TimeMeasure{request.windowSizeMs}};
}
}

StatisticRetrievalService::StatisticRetrievalService(StatisticCoordinator& coordinator) : coordinator(coordinator)
{
}

void StatisticRetrievalService::deployStatisticBuildIfAbsent() const
{
    /// Deduplicated by the coordinator: the first call deploys the continuous build query, later calls return the
    /// existing entry without redeploying.
    if (const auto result = coordinator.collectNewStatistic(mockBuildRequest()); not result.has_value())
    {
        NES_WARNING("StatisticRetrievalService: failed to deploy statistic build query: {}", result.error().what());
    }
}

std::optional<double> StatisticRetrievalService::retrieveStatistic() const
{
    /// Probe the already-running build. The build writes (statisticId, 0, windowSizeMs, value), so we probe that
    /// exact window. getStatistics throws if no build is registered for the key (none was deployed yet); treat that
    /// as "no statistic available" rather than an error.
    try
    {
        return coordinator.getStatistics(mockKey(), Windowing::TimeMeasure{0}, Windowing::TimeMeasure{MOCK_WINDOW_SIZE_MS});
    }
    catch (const Exception& exception)
    {
        NES_DEBUG("StatisticRetrievalService: no running build to probe: {}", exception.what());
        return std::nullopt;
    }
}

}
