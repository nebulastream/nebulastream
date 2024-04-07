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

#include <QueryCompiler/Operators/PhysicalOperators/StatisticCollection/PhysicalSynopsisBuildOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

const std::string& PhysicalSynopsisBuildOperator::getNameOfFieldToTrack() const {
    return nameOfFieldToTrack;
}

Statistic::MetricHash PhysicalSynopsisBuildOperator::getMetricHash() const { return metricHash; }

const Windowing::WindowTypePtr& PhysicalSynopsisBuildOperator::getWindowType() const { return windowType; }

const Statistic::SendingPolicyPtr& PhysicalSynopsisBuildOperator::getSendingPolicy() const { return sendingPolicy; }

PhysicalSynopsisBuildOperator::PhysicalSynopsisBuildOperator(const std::string_view nameOfFieldToTrack,
                                                             const Statistic::MetricHash metricHash,
                                                             const Windowing::WindowTypePtr windowType,
                                                             const Statistic::SendingPolicyPtr sendingPolicy)
    : nameOfFieldToTrack(nameOfFieldToTrack), metricHash(metricHash), windowType(windowType), sendingPolicy(sendingPolicy) {}
} // namespace NES::QueryCompilation::PhysicalOperators