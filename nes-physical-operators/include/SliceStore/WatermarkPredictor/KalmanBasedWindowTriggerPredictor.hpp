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

// TODO add to cmake as library
#include <mutex>
#include <../../eigen/Dense>
#include <SliceStore/WatermarkPredictor/AbstractWatermarkPredictor.hpp>

namespace NES
{

class KalmanWindowTriggerPredictor final : public AbstractWatermarkPredictor
{
public:
    explicit KalmanWindowTriggerPredictor(uint64_t initial = 0);

    void update(const std::vector<std::pair<uint64_t, Timestamp::Underlying>>& data) override;
    [[nodiscard]] Timestamp getEstimatedWatermark(uint64_t timestamp) const override;

private:
    mutable std::mutex mtx;

    Eigen::Vector2d state;
    Eigen::Matrix2d P;
    Eigen::Matrix2d Q;
    Eigen::RowVector2d H;
    uint64_t lastX;
    double R;
};

}
