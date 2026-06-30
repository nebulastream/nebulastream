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
#include <string>
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
/// A statistic is identified by the source it is collected on (plus a fixed mock field/metric/window). The build
/// and the probe for a given source therefore refer to the same registry key, so the probe finds the build the
/// coordinator registered. In a real system the metric/field/window would be derived from the query; here they are
/// fixed stand-ins and only the source distinguishes statistics.
constexpr uint64_t MOCK_WINDOW_SIZE_MS = 1000;

RequestStatisticBuildStatement buildRequestFor(const std::string& source)
{
    return RequestStatisticBuildStatement{
        .domain = DataDomain{.logicalSourceName = source, .fieldName = "value"},
        .metric = Metric::Average,
        .windowSizeMs = MOCK_WINDOW_SIZE_MS,
        .windowAdvanceMs = std::nullopt,
        .eventTimeFieldName = std::nullopt,
        .conditionTrigger = std::nullopt,
        .options = {}};
}

StatisticRegistry::Key keyFor(const std::string& source)
{
    const auto request = buildRequestFor(source);
    return StatisticRegistry::Key{
        .metric = request.metric, .collectionDomain = request.domain, .windowSize = Windowing::TimeMeasure{request.windowSizeMs}};
}
}

StatisticRetrievalService::StatisticRetrievalService(StatisticCoordinator& coordinator) : coordinator(coordinator)
{
}

void StatisticRetrievalService::deployStatisticBuild(const std::string& source) const
{
    /// Deduplicated by the coordinator on the source-derived key: collecting a new source deploys a fresh build,
    /// re-requesting a source already being collected reuses the running build.
    if (const auto result = coordinator.collectNewStatistic(buildRequestFor(source)); not result.has_value())
    {
        NES_WARNING("StatisticRetrievalService: failed to deploy statistic build query for source {}: {}", source, result.error().what());
    }
}

std::optional<double> StatisticRetrievalService::retrieveStatistic(const std::string& source) const
{
    /// Probe the build collecting `source`. The build writes (statisticId, 0, windowSizeMs, value), so we probe that
    /// exact window. getStatistics throws if no build is registered for the source; treat that as "no statistic
    /// available" rather than an error.
    try
    {
        return coordinator.getStatistics(keyFor(source), Windowing::TimeMeasure{0}, Windowing::TimeMeasure{MOCK_WINDOW_SIZE_MS});
    }
    catch (const Exception& exception)
    {
        NES_DEBUG("StatisticRetrievalService: no running build to probe for source {}: {}", source, exception.what());
        return std::nullopt;
    }
}

}
