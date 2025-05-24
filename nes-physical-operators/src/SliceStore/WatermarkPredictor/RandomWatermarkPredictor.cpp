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

#include <SliceStore/WatermarkPredictor/RandomWatermarkPredictor.hpp>

namespace NES
{

RandomWatermarkPredictor::RandomWatermarkPredictor(const uint64_t initial, const uint64_t seed)
    : AbstractWatermarkPredictor(initial), min(UINT64_MAX), max(0)
{
    if (seed == 0)
    {
        std::random_device rd;
        gen.seed(rd());
    }
    gen.seed(seed);
}

void RandomWatermarkPredictor::update(const std::vector<std::pair<uint64_t, Timestamp::Underlying>>& data)
{
    min = UINT64_MAX;
    max = 0;

    for (const auto& [_, watermark] : data)
    {
        if (watermark < min)
        {
            min = watermark;
        }
        else if (watermark > max)
        {
            max = watermark;
        }
    }
    init = true;
}

Timestamp RandomWatermarkPredictor::getEstimatedWatermark(const uint64_t) const
{
    if (!init)
    {
        return Timestamp(initial);
    }
    std::uniform_int_distribution<> dis(min, max);
    //return Timestamp(dis(gen));
    return Timestamp(0);
}

}
