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

#include <random>
#include <SliceStore/WatermarkPredictor/KalmanBasedWatermarkPredictor.hpp>
#include <SliceStore/WatermarkPredictor/RLSBasedWatermarkPredictor.hpp>
#include <SliceStore/WatermarkPredictor/RegressionBasedWatermarkPredictor.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

static std::random_device rd;
static std::mt19937 gen(rd());

class WatermarkPredictorTest : public Testing::BaseUnitTest
{
public:
    static constexpr auto NUM_RUNS = 10;
    static constexpr auto SIZES = {10, 100, 1000, 10000, 100000, 1000000, 10000000};
    static constexpr auto RANGES = {5, 50, 500, 5000, 50000};

    static void SetUpTestSuite()
    {
        Logger::setupLogging("WatermarkPredictorTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup WatermarkPredictorTest class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }

    static std::vector<std::pair<uint64_t, Timestamp::Underlying>> generateData(const size_t size, const int noiseRange)
    {
        std::uniform_int_distribution dist(-noiseRange, noiseRange);
        std::vector<std::pair<uint64_t, Timestamp::Underlying>> data;
        data.reserve(size);

        for (auto i = 0UL; i < size; ++i)
        {
            data.emplace_back(i, std::max(i + dist(gen), 0UL));
        }

        return data;
    }

    static std::string watermarkTypeToString(const WatermarkPredictorType type)
    {
        switch (type)
        {
            case WatermarkPredictorType::KALMAN:
                return "KALMAN";
            case WatermarkPredictorType::REGRESSION:
                return "REGRESSION";
            case WatermarkPredictorType::RLS:
                return "RLS";
            default:
                throw std::runtime_error("Unknown watermark predictor type");
        }
    }
};

TEST_F(WatermarkPredictorTest, update)
{
    std::ofstream file("WatermarkPredictorTestResult.csv", std::ios::out | std::ios::trunc | std::ios::binary);
    if (not file.is_open())
    {
        throw std::runtime_error(fmt::format("Failed to open file: {}", std::strerror(errno)));
    }

    const std::vector<std::pair<WatermarkPredictorType, std::shared_ptr<AbstractWatermarkPredictor>>> predictors
        = {{WatermarkPredictorType::KALMAN, std::make_shared<KalmanBasedWatermarkPredictor>()},
           {WatermarkPredictorType::REGRESSION, std::make_shared<RegressionBasedWatermarkPredictor>()},
           {WatermarkPredictorType::RLS, std::make_shared<RLSBasedWatermarkPredictor>()}};

    for (auto i = 0; i < NUM_RUNS; ++i)
    {
        for (const auto size : SIZES)
        {
            for (const auto range : RANGES)
            {
                const auto data = generateData(size, range);
                for (const auto& [type, predictor] : predictors)
                {
                    // TODO is there a difference if state is not cleared?
                    const auto start = std::chrono::high_resolution_clock::now();
                    predictor->update(data);
                    const auto end = std::chrono::high_resolution_clock::now();

                    const auto startTicks
                        = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(start.time_since_epoch()).count());
                    const auto endTicks
                        = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(end.time_since_epoch()).count());
                    file << watermarkTypeToString(type) << "," << (endTicks - startTicks) << "," << size << "," << range << "\n";
                }
            }
        }
    }
}

}
