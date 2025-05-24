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

#include <SliceStore/WatermarkPredictor/KalmanBasedWindowTriggerPredictor.hpp>

namespace NES
{

KalmanWindowTriggerPredictor::KalmanWindowTriggerPredictor(const uint64_t initial) : AbstractWatermarkPredictor(initial), lastX(0), R(1e-2)
{
    state << 0, 0;
    P = Eigen::Matrix2d::Identity();
    Q = Eigen::Matrix2d::Identity() * 1e-5;
    H << 1, 0;
}

void KalmanWindowTriggerPredictor::update(const std::vector<std::pair<uint64_t, Timestamp::Underlying>>& data)
{
    const std::lock_guard lock(mtx);
    for (const auto& [timestamp, watermark] : data)
    {
        const auto dx = timestamp - lastX;
        lastX = timestamp;

        /// Prediction
        Eigen::Matrix2d F;
        F << 1, dx, 0, 1;

        state = F * state;
        P = F * P * F.transpose() + Q;

        /// Measurement update
        const auto z = watermark;
        const auto y_residual = z - H * state;
        const auto S = (H * P * H.transpose())(0, 0) + R;
        const Eigen::Vector2d K = P * H.transpose() / S;

        state = state + K * y_residual;
        P = (Eigen::Matrix2d::Identity() - K * H) * P;
    }
    init = true;
}

Timestamp KalmanWindowTriggerPredictor::getEstimatedWatermark(const uint64_t timestamp) const
{
    if (!init)
    {
        return Timestamp(initial);
    }

    const std::lock_guard lock(mtx);
    const auto dx = timestamp - lastX;
    return Timestamp(state(0) + dx * state(1));
}

}
