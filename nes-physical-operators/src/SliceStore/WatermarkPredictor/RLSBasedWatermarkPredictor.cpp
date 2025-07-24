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

#include <SliceStore/WatermarkPredictor/RLSBasedWatermarkPredictor.hpp>

namespace NES
{

RLSBasedWatermarkPredictor::RLSBasedWatermarkPredictor(const uint64_t initial, const uint64_t initCovariance, const double lambda)
    : AbstractWatermarkPredictor(initial), lambda(lambda)
{
    weight = Eigen::Vector2d::Zero(2);
    covariance = Eigen::Matrix2d::Identity(2, 2) * initCovariance;

    const auto now = std::chrono::high_resolution_clock::now();
    baseTime = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count());
}

void RLSBasedWatermarkPredictor::update(const std::vector<std::pair<uint64_t, Timestamp::Underlying>>& data)
{
    if (data.empty())
    {
        return;
    }

    const auto [weightLocked, covarianceLocked] = acquireLocked(weight, covariance);
    for (const auto& [timestamp, watermark] : data)
    {
        Eigen::Vector2d phi;
        phi << 1, static_cast<double>(timestamp - baseTime);

        const Eigen::Vector2d K = *covarianceLocked * phi / (lambda + phi.transpose() * *covarianceLocked * phi);
        *weightLocked = *weightLocked + K * (watermark - phi.dot(*weightLocked));
        *covarianceLocked = (*covarianceLocked - K * phi.transpose() * *covarianceLocked) / lambda;
    }
}

Timestamp RLSBasedWatermarkPredictor::getEstimatedWatermark(const uint64_t timestamp) const
{
    Eigen::Vector2d phi;
    phi << 1, static_cast<double>(timestamp - baseTime);

    const auto weightLocked = weight.rlock();
    return Timestamp(phi.dot(*weightLocked));
}

}
