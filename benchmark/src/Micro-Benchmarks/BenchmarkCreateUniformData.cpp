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

#include <util/BenchmarkUtils.hpp>

static std::string fileName = "create_uniform_data";

using namespace NES::Benchmarking;

/*
 * This main calculates mean and stddev of BenchmarkUtils::createRangeVector. This is a sanity check as BenchmarkUtils::createRangeVector
 * may contain only a small number of items
 */
int main() {

    // Optimal mean would be 499.5 and optimal stddev would be 288.38
    // These values result from the fact that BenchmarkUtils::createUniformData provides uniform distribution in the range [0, 999]

    double optimalMean = (0 + 999) / 2.0;
    double optimalStddev = (0 + 999) / std::sqrt(12.0);

    std::ofstream file;
    file.open((fileName) + "_results.csv", std::ios_base::trunc);
    file << "ID, ListSize, AbsErrorMean, AbsErrorStddev, MaxAbsErrorMean, MaxAbsErrorStddev\n";
    file.close();

    uint64_t cntListSize = 0;

    constexpr uint64_t REPS = 1e3;
    std::vector<uint64_t> allListSizes;
    BenchmarkUtils::createRangeVector<uint64_t>(allListSizes, 100, 10000, 100);

    for (auto listSize : allListSizes) {

        double absMeanError = 0;
        double absStddev = 0;
        double maxMeanErrorMean = 0;
        double maxMeanErrorStddev = 0;

        for (uint64_t i = 0; i < REPS; ++i) {
            std::list<uint64_t> keyList;
            BenchmarkUtils::createUniformData(keyList, listSize);

            // Sum over all items in keyList for calculating mean
            uint64_t tmpSum = 0;
            for (auto it : keyList) {
                tmpSum += it;
            }
            double mean = (double) tmpSum / keyList.size();

            // (x - mean)^2 over all items in keyList for calculating stddev
            tmpSum = 0;
            for (auto it : keyList) {
                tmpSum += std::pow(it - mean, 2);
            }

            double stddev = std::sqrt((double) tmpSum / keyList.size());
            double curErrMean = (optimalMean - mean);
            double curErrStddev = (optimalStddev - stddev);

            maxMeanErrorMean = (std::abs(maxMeanErrorMean) > std::abs(curErrMean)) ? maxMeanErrorMean : curErrMean;
            maxMeanErrorStddev = (std::abs(maxMeanErrorStddev) > std::abs(curErrStddev)) ? maxMeanErrorStddev : curErrStddev;

            // Sum over meanError and stddev to average over REPS
            absMeanError += curErrMean;
            absStddev += curErrStddev;
        }

        file.open((fileName) + "_results.csv", std::ios_base::app);
        file << cntListSize << "," << listSize << "," << (absMeanError / REPS) << "," << (absStddev / REPS) << ","
             << (maxMeanErrorMean) << "," << (maxMeanErrorStddev) << "\n";
        file.close();

        ++cntListSize;
        std::cout << "\rListsize=" << listSize << " done [" << cntListSize << "/" << allListSizes.size() << "]";
        fflush(stdout);
    }

    return 0;
}