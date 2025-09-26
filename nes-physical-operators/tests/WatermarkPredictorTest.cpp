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
    static constexpr auto RANGES = {0, 5, 50, 500, 5000, 50000};

    static void SetUpTestSuite()
    {
        Logger::setupLogging("WatermarkPredictorTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup WatermarkPredictorTest class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }

    static std::vector<std::pair<uint64_t, Timestamp::Underlying>> generateData(const size_t size, const int noiseRange)
    {
        std::uniform_int_distribution dist(0, noiseRange);
        std::vector<std::pair<uint64_t, Timestamp::Underlying>> data;
        data.reserve(size);

        for (auto i = 0UL; i < size; ++i)
        {
            const auto noiseValue = dist(gen);
            if (noiseValue < 0)
            {
                data.emplace_back(i, std::max(i + dist(gen), 0UL));
            }
            else
            {
                data.emplace_back(i, i); // std::max(i + dist(gen), 0UL));
            }
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

    static std::shared_ptr<AbstractWatermarkPredictor> createWatermarkPredictor(const WatermarkPredictorType type)
    {
        switch (type)
        {
            case WatermarkPredictorType::KALMAN:
                return std::make_shared<KalmanBasedWatermarkPredictor>();
            case WatermarkPredictorType::REGRESSION:
                return std::make_shared<RegressionBasedWatermarkPredictor>();
            case WatermarkPredictorType::RLS:
                return std::make_shared<RLSBasedWatermarkPredictor>();
            default:
                throw std::runtime_error("Unknown watermark predictor type");
        }
    }
};

TEST_F(WatermarkPredictorTest, update)
{
    std::ofstream file("WatermarkPredictorTestResult.csv", std::ios::out | std::ios::trunc);
    if (not file.is_open())
    {
        throw std::runtime_error(fmt::format("Failed to open file: {}", std::strerror(errno)));
    }
    file << "WatermarkPredictor,UpdateSize,NoiseRange,UpdateExecutionTime,PredictionExecutionTime,timestampToPredict,ValueForTimestamp,"
            "PredictionForTimestamp,Deviation\n";

    const std::vector predictorTypes = {WatermarkPredictorType::KALMAN, WatermarkPredictorType::REGRESSION, WatermarkPredictorType::RLS};
    for (auto i = 0; i < NUM_RUNS; ++i)
    {
        for (const auto range : RANGES)
        {
            for (const auto size : SIZES)
            {
                std::uniform_int_distribution dist(0, size);
                const auto timestampToPredict = static_cast<uint64_t>(dist(gen));
                const auto data = generateData(size, range);
                for (const auto type : predictorTypes)
                {
                    const auto predictor = createWatermarkPredictor(type);
                    const auto startWrite = std::chrono::high_resolution_clock::now();
                    predictor->update(data);
                    const auto endWrite = std::chrono::high_resolution_clock::now();
                    const auto prediction = predictor->getEstimatedWatermark(timestampToPredict);
                    const auto endPrediction = std::chrono::high_resolution_clock::now();

                    const auto startWriteTicks = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(startWrite.time_since_epoch()).count());
                    const auto endWriteTicks
                        = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(endWrite.time_since_epoch()).count());
                    const auto endPredictionTicks = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(endPrediction.time_since_epoch()).count());

                    file << watermarkTypeToString(type) << "," << size << "," << range << "," << (endWriteTicks - startWriteTicks) << ","
                         << (endPredictionTicks - endWriteTicks) << "," << timestampToPredict << "," << data[timestampToPredict].second
                         << "," << prediction.getRawValue() << "," << (data[timestampToPredict].second - prediction.getRawValue()) << "\n";
                }
            }
        }
    }
}

TEST_F(WatermarkPredictorTest, DISABLED_update_2)
{
    std::ofstream file("WatermarkPredictorTestResult_2.csv", std::ios::out | std::ios::trunc);
    if (not file.is_open())
    {
        throw std::runtime_error(fmt::format("Failed to open file: {}", std::strerror(errno)));
    }

    const std::vector predictorTypes = {WatermarkPredictorType::KALMAN, WatermarkPredictorType::REGRESSION, WatermarkPredictorType::RLS};
    for (auto i = 0; i < NUM_RUNS; ++i)
    {
        for (const auto range : RANGES)
        {
            for (const auto type : predictorTypes)
            {
                const auto predictor = createWatermarkPredictor(type);
                for (const auto size : SIZES)
                {
                    std::uniform_int_distribution dist(0, size);
                    const auto timestampToPredict = static_cast<uint64_t>(dist(gen));
                    const auto data = generateData(size, range);

                    const auto startWrite = std::chrono::high_resolution_clock::now();
                    predictor->update(data);
                    const auto endWrite = std::chrono::high_resolution_clock::now();
                    const auto prediction = predictor->getEstimatedWatermark(timestampToPredict);
                    const auto endPrediction = std::chrono::high_resolution_clock::now();

                    const auto startWriteTicks = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(startWrite.time_since_epoch()).count());
                    const auto endWriteTicks
                        = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(endWrite.time_since_epoch()).count());
                    const auto endPredictionTicks = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(endPrediction.time_since_epoch()).count());

                    file << watermarkTypeToString(type) << "," << size << "," << range << "," << (endWriteTicks - startWriteTicks) << ","
                         << (endPredictionTicks - endWriteTicks) << "," << timestampToPredict << ","
                         << (data[timestampToPredict].second - prediction.getRawValue()) << "\n";
                }
            }
        }
    }
}

}
