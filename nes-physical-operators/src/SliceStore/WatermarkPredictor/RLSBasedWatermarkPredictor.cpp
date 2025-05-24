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
    weight = Eigen::VectorXd::Zero(2);
    covariance = Eigen::MatrixXd::Identity(2, 2) * initCovariance;
}

void RLSBasedWatermarkPredictor::update(const std::vector<std::pair<uint64_t, Timestamp::Underlying>>& data)
{
    const auto n = data.size();
    if (n < 1)
    {
        return;
    }

    const auto [weightLocked, covarianceLocked] = acquireLocked(weight, covariance);
    for (auto i = 0UL; i < n; ++i)
    {
        Eigen::VectorXd phi(2); /// Design vector
        phi << 1, data[i].first;

        Eigen::VectorXd K = (*covarianceLocked * phi) / (lambda + phi.transpose() * *covarianceLocked * phi);
        *weightLocked = *weightLocked + K * (data[i].second - phi.dot(*weightLocked));
        *covarianceLocked = (*covarianceLocked - K * phi.transpose() * *covarianceLocked) / lambda;
    }

    init = true;
}

Timestamp RLSBasedWatermarkPredictor::getEstimatedWatermark(const uint64_t timestamp) const
{
    if (!init)
    {
        return Timestamp(initial);
    }

    Eigen::VectorXd newX(2);
    newX << 1, timestamp;

    const auto weightLocked = weight.rlock();
    return Timestamp(newX.dot(*weightLocked));
    //return Timestamp((*weightLocked)(0) + (*weightLocked)(1) * timestamp);
}

}
