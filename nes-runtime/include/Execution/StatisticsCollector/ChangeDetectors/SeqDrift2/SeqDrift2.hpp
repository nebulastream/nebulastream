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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_STATISTICSCOLLECTOR_CHANGEDETECTORS_SEQDRIFT2_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_STATISTICSCOLLECTOR_CHANGEDETECTORS_SEQDRIFT2_HPP_

#include <Execution/StatisticsCollector/ChangeDetectors/SeqDrift2/Reservoir.hpp>
#include <Execution/StatisticsCollector/ChangeDetectors/ChangeDetector.hpp>
#include <vector>

namespace NES::Runtime::Execution {

class SeqDrift2 : public ChangeDetector {
  public:
    /**
     * @brief SeqDrift change detector requires two parameters: significance level and block size.
     * @param significanceLevel value between 0 and 1, controls the false positive rate
     * @param blockSize sets the interval of two consecutive hypothesis tests
     */
    SeqDrift2(double significanceLevel, int blockSize);

    /**
     * @brief Insert value into SeqDrift change detector.
     * @param value value between 0 and 1
     * @return true, if change was detected.
     */
    bool insertValue(double& value) override;

    /**
     * @brief Get estimated mean.
     * @return estimated mean.
     */
    double getMeanEstimation() override;

  private:

    /**
     * @brief Adds a new value to right reservoir.
     * @param value
     * @return void
     */
    void addToRightReservoir(double value);

    /**
     * @brief Removes all elements from the reservoir after a drift is detected.
     */
    void clearLeftReservoir();

    /**
     * @brief Copies the values of the right repository to the left reservoir applying reservoir sampling algorithm.
     */
    void moveFromRepositoryToReservoir();

    /**
     * @brief Get the type of drift detected.
     * @return DRIFT or NODRIFT.
     */
    int getDriftType();
    double getRightRepositoryMean();
    double getLeftReservoirMean();
    double getMean();
    int getWidth();
    double getTotal();
    double getVariance();

    /**
     * @brief
     */
    void optimizeEpsilon();

    /**
     * @brief The effect of multiple tests is to reduce the confidence from δ to δ′ which represents
     * the effective (overall) confidence after n successive hypothesis tests have been carried.
     * @param iNumTests number of successive hypothesis tests
     * @return new confidence value delta dash
     */
    double getDriftEpsilon(int iNumTests);

    /**
     * @brief Calculate the optimal k value based on rate of change.
     * @param dKr
     * @return new k value.
     */
    double adjustForDataRate(double dKr);

    Reservoir right;
    Reservoir left;
    const int rightRepositorySize;
    int leftReservoirSize;
    const int blockSize;
    const double significanceLevel;
    const double k;

    int instanceCount;
    double rightRepositoryMean;
    double leftReservoirMean;
    double variance;
    double total;
    double epsilon;

    const static int DRIFT = 0;
    const static int NODRIFT = 2;

};

} // namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_STATISTICSCOLLECTOR_CHANGEDETECTORS_SEQDRIFT2_HPP_