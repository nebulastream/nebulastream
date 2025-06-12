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

#include <cstddef>
#include <vector>
#include <Util/RollingAverage.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
namespace NES
{

class RollingAverageTest : public ::testing::Test
{
protected:
    template <typename T>
    static void testRollingAverage(const std::vector<T>& inputs, const std::vector<double>& expectedAverages, size_t windowSize)
    {
        RollingAverage<T> rollingAvg(windowSize);

        /// Ensure the number of inputs and expected averages match
        ASSERT_EQ(inputs.size(), expectedAverages.size());

        for (size_t i = 0; i < inputs.size(); ++i)
        {
            EXPECT_DOUBLE_EQ(rollingAvg.add(inputs[i]), expectedAverages[i]);
            EXPECT_DOUBLE_EQ(rollingAvg.getAverage(), expectedAverages[i]);
        }
    }
};

TEST_F(RollingAverageTest, BasicTest)
{
    const std::vector inputs = {1, 2, 3, 4, 5};
    const std::vector expectedAverages = {1.0, 1.5, 2.0, 3.0, 4.0};
    testRollingAverage(inputs, expectedAverages, 3);
}

TEST_F(RollingAverageTest, AllIdenticalElements)
{
    const std::vector inputs = {2, 2, 2, 2, 2};
    const std::vector expectedAverages = {2.0, 2.0, 2.0, 2.0, 2.0};
    testRollingAverage(inputs, expectedAverages, 3);
}

TEST_F(RollingAverageTest, IncreasingSequence)
{
    const std::vector inputs = {10, 20, 30, 40, 50};
    const std::vector expectedAverages = {10.0, 15.0, 25.0, 35.0, 45.0};
    testRollingAverage(inputs, expectedAverages, 2);
}

TEST_F(RollingAverageTest, DecreasingSequence)
{
    const std::vector inputs = {50, 40, 30, 20, 10};
    const std::vector expectedAverages = {50.0, 45.0, 40.0, 35.0, 25.0};
    testRollingAverage(inputs, expectedAverages, 4);
}

TEST_F(RollingAverageTest, SingleElementWindow)
{
    const std::vector inputs = {7, 8, 9, 10};
    const std::vector expectedAverages = {7.0, 8.0, 9.0, 10.0};
    testRollingAverage(inputs, expectedAverages, 1);
}

TEST_F(RollingAverageTest, WindowSizeLargerThanInput)
{
    const std::vector inputs = {1, 2, 3};
    const std::vector expectedAverages = {1.0, 1.5, 2.0};
    testRollingAverage(inputs, expectedAverages, 5);
}

TEST_F(RollingAverageTest, NegativeNumbers)
{
    const std::vector inputs = {-1, -2, -3, -4, -5};
    const std::vector expectedAverages = {-1.0, -1.5, -2.0, -3.0, -4.0};
    testRollingAverage(inputs, expectedAverages, 3);
}

TEST_F(RollingAverageTest, MixedPositiveAndNegativeNumbers)
{
    const std::vector inputs = {-1, 2, -3, 4, -5};
    const std::vector expectedAverages = {-1.0, 0.5, -0.5, 0.5, -0.5};
    testRollingAverage(inputs, expectedAverages, 2);
}

TEST_F(RollingAverageTest, EmptyInput)
{
    const std::vector<int> inputs = {};
    const std::vector<double> expectedAverages = {};
    testRollingAverage(inputs, expectedAverages, 3);
}

}
