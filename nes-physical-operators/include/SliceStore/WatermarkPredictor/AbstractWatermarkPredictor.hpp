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

#include <atomic>
#include <map>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>

namespace NES
{

enum class WatermarkPredictorType : uint8_t
{
    KALMAN,
    REGRESSION,
    RLS
};

struct WatermarkPredictorInfo
{
    WatermarkPredictorType type;
    uint64_t initial;
    uint64_t param1;
    double param2;
};

class AbstractWatermarkPredictor
{
public:
    explicit AbstractWatermarkPredictor(uint64_t initial);
    virtual ~AbstractWatermarkPredictor() = default;

    virtual void update(const std::vector<std::pair<uint64_t, Timestamp::Underlying>>& data) = 0;
    [[nodiscard]] virtual Timestamp getEstimatedWatermark(uint64_t timestamp) const = 0;

    static Timestamp getMinPredictedWatermarkForTimestamp(
        const std::map<OriginId, std::shared_ptr<AbstractWatermarkPredictor>>& watermarkPredictors, uint64_t timestamp);

protected:
    uint64_t initial;
    std::atomic<bool> init;
};

}
