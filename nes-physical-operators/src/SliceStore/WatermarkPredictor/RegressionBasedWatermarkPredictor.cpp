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

#include <SliceStore/WatermarkPredictor/RegressionBasedWatermarkPredictor.hpp>

namespace NES
{

RegressionBasedWatermarkPredictor::RegressionBasedWatermarkPredictor(const uint64_t initial, const uint64_t degree)
    : AbstractWatermarkPredictor(initial), degree(degree)
{
}

void RegressionBasedWatermarkPredictor::update(const std::vector<std::pair<uint64_t, Timestamp::Underlying>>& data)
{
    const auto n = data.size();
    if (n < 1)
    {
        return;
    }

    Eigen::MatrixXd X(n, degree + 1);
    Eigen::VectorXd y(n);

    for (auto i = 0UL; i < n; ++i)
    {
        X(i, 0) = 1; /// Bias term
        for (auto j = 1UL; j <= degree; ++j)
        {
            X(i, j) = std::pow(data[i].first, j); /// Polynomial terms
        }
        y(i) = data[i].second; /// Watermark
    }

    const auto coefficientsLocked = coefficients.wlock();
    *coefficientsLocked = (X.transpose() * X).ldlt().solve(X.transpose() * y);
    init = true;
}

Timestamp RegressionBasedWatermarkPredictor::getEstimatedWatermark(const uint64_t timestamp) const
{
    if (!init)
    {
        return Timestamp(initial);
    }

    Eigen::VectorXd newX(degree + 1);
    newX(0) = 1; /// Bias term
    for (auto j = 1UL; j <= degree; ++j)
    {
        newX(j) = std::pow(timestamp, j); /// Polynomial terms
    }

    const auto coefficientsLocked = coefficients.rlock();
    return Timestamp(coefficientsLocked->dot(newX));
}

}
