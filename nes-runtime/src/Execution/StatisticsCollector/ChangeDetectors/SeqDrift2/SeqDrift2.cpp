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
#include <Execution/StatisticsCollector/ChangeDetectors/SeqDrift2/SeqDrift2.hpp>
#include <cmath>

namespace NES::Runtime::Execution {

SeqDrift2::SeqDrift2(double significanceLevel, int blockSize):
  right(blockSize, blockSize),
  left(blockSize, blockSize),
  rightRepositorySize(blockSize), // left sub-window: reservoir that stores values that are statistically not different
  leftReservoirSize(blockSize), // right sub-window: repository that stores new instances
  blockSize(blockSize),
  significanceLevel(significanceLevel),
  k(0.5),
  instanceCount(0),
  variance(0.0),
  total(0.0),
  epsilon(0.0)
{}

bool SeqDrift2::insertValue(double &inputValue) {
    instanceCount++;

    addToRightReservoir(inputValue); // add new value to right sub-window
    total += inputValue;

    if((instanceCount % blockSize) == 0){ // check for drift at block boundary
        int driftType = getDriftType();
        if (driftType == DRIFT){
            clearLeftReservoir(); // remove all instances in left sub-window
            moveFromRepositoryToReservoir(); // move instances from right sub-window to left sub-window
            return true;
        } else { // no drift detected
            moveFromRepositoryToReservoir(); // concatenate block of right sub-window to left sub-window
            return false;
        }
    }
    return false;
}

void SeqDrift2::addToRightReservoir(double value) {
    right.addElement(value);
}

void SeqDrift2::clearLeftReservoir() {
    total = total - left.getTotal();
    left.clear();
}

void SeqDrift2::moveFromRepositoryToReservoir() {
    left.copy(right);
}

int SeqDrift2::getDriftType() {
    if (getWidth() > blockSize){
        rightRepositoryMean = getRightRepositoryMean();
        leftReservoirMean = getLeftReservoirMean();
        optimizeEpsilon();

        if ((instanceCount > blockSize) && (left.getSize() > 0)){
            if (epsilon <= std::abs(rightRepositoryMean - leftReservoirMean)){
                return DRIFT;
            } else {
                return NODRIFT;
            }
        }
        return NODRIFT;
    } else {
        return NODRIFT;
    }
}

double SeqDrift2::getRightRepositoryMean() {
    return right.getSampleMean();
}

double SeqDrift2::getLeftReservoirMean() {
    return left.getSampleMean();
}

double SeqDrift2::getMean() {
    return getTotal() / getWidth();
}

int SeqDrift2::getWidth() {
    return right.getSize() + left.getSize();
}

double SeqDrift2::getTotal() {
    return right.getTotal() + left.getTotal();
}

double SeqDrift2::getVariance() {
    double mean = getMean();
    double size = getWidth();
    double x = getTotal() * (mean - 1) * (mean - 1) + (size - getTotal()) * mean * mean; //TODO size - total is negative?
    double y = size - 1;
    return x / y;
}

void SeqDrift2::optimizeEpsilon() {
    int tests = left.getSize() / blockSize;

    if (tests >= 1){
        variance = getVariance();
        if (variance == 0){
            variance = 0.0001; // to avoid divide by zero exception
        }

        double ddeltadash = getDriftEpsilon(tests);
        double x = log(4.0 / ddeltadash);
        double ktemp = k; // initial k = 0.5

        double previousStepEpsilon;
        double currentStepEpsilon;
        double squareRootValue = 0.0;

        bool isNotOptimized = true;
        while (isNotOptimized){
            squareRootValue = sqrt(x * x + 18 * rightRepositorySize * x * variance);
            previousStepEpsilon = (1.0 / (3 * rightRepositorySize * (1 - ktemp))) * (x + squareRootValue);
            ktemp = 3 * ktemp / 4; // decrement k in small steps
            currentStepEpsilon = (1.0 / (3 * rightRepositorySize * (1 - ktemp))) * (x + squareRootValue);

            if (((previousStepEpsilon - currentStepEpsilon) / previousStepEpsilon) < 0.0001) {
                isNotOptimized = false;
            }
        }

        ktemp = 4 * ktemp / 3; // reset k as it decreased beyond the optimal value
        ktemp = adjustForDataRate(ktemp); // adjust k according to rate of change in data
        leftReservoirSize = (int) (rightRepositorySize * (1 - ktemp) / ktemp); // set left reservoir size according to new k
        left.setSampleSize(leftReservoirSize);
        squareRootValue = sqrt(x * x + 18 * rightRepositorySize * x * variance);
        currentStepEpsilon = (1.0 / (3 * rightRepositorySize * (1 - ktemp))) * (x + squareRootValue); //TODO see if necessary
        epsilon = currentStepEpsilon;

    }
}

double SeqDrift2::getDriftEpsilon(int iNumTests) {
    double dSeriesTotal = 2.0 * (1.0 - pow(0.5, iNumTests));
    double ddeltadash = significanceLevel / dSeriesTotal; // new confidence
    return ddeltadash;
}

double SeqDrift2::adjustForDataRate(double dKr) {
    double meanIncrease = (right.getSampleMean() - left.getSampleMean()); // determine current rate of change
    double dk = dKr;
    if (meanIncrease > 0) {
        dk = dk + ((-1) * (meanIncrease * meanIncrease * meanIncrease * meanIncrease) + 1) * dKr;
    } else if (meanIncrease <= 0) {
        dk = dKr;
    }
    return dk;
}

double SeqDrift2::getMeanEstimation() {
    int width = getWidth();

    if (width != 0){
        return  getTotal() / width;
    } else {
        return 0;
    }
}

void SeqDrift2::reset() {
    left.clear();
    right.clear();
    k = 0.5;
    instanceCount = 0;
    variance = 0.0;
    total= 0.0;
    epsilon = 0.0;
}

} // namespace NES::Runtime::Execution