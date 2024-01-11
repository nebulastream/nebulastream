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

#ifndef NES_BENCHMARK_INCLUDE_UTIL_ZIPFIANGENERATOR_HPP_
#define NES_BENCHMARK_INCLUDE_UTIL_ZIPFIANGENERATOR_HPP_

#include <random>

// Inspired by https://github.com/brianfrankcooper/YCSB/blob/master/core/src/main/java/site/ycsb/generator/ZipfianGenerator.java
class ZipfianGenerator {
    static constexpr auto DEFAULT_ZIPFIAN_GENERATOR = .99;

  public:
    /**
     * @brief Constructs a ZipfianGenerator
     * @param min
     * @param max
     * @param zipfianFactor
     */
    explicit ZipfianGenerator(uint64_t min, uint64_t max, double zipfianFactor = DEFAULT_ZIPFIAN_GENERATOR)
        : ZipfianGenerator(min, max, zipfianFactor, computeZeta(0, max - min + 1, zipfianFactor, 0)) {}

    explicit ZipfianGenerator(uint64_t min, uint64_t max, double zipfianConstant, double zetan_) : dist(0.0, 1.0) {
        numItems = max - min + 1;
        this->min = min;
        this->zipfianConstant = zipfianConstant;
        this->theta = this->zipfianConstant;

        zeta2Theta = zeta(0, 2, theta, 0);

        alpha = 1.0 / (1.0 - theta);
        zetan = zetan_;
        countForZeta = numItems;
        eta = (1 - std::pow(2.0 / numItems, 1 - theta)) / (1 - zeta2Theta / zetan);
    }

    /**
     * @brief overrides the operator() and generates a new random value drawn from a Zipfian distribution
     * @param rng
     * @return random value
     */
    uint64_t operator()(std::mt19937& rng) { return (*this)(rng, countForZeta); }

    /**
     * @brief overrides the operator() and generates a new random value drawn from a Zipfian distribution
     * @param rng
     * @return random value
     */
    uint64_t operator()(std::mt19937& rng, uint64_t newItemCount) {
        if (newItemCount > countForZeta) {
            // we have added more items. can compute zetan incrementally, which is cheaper
            numItems = newItemCount;
            zetan = zeta(countForZeta, numItems, theta, zetan);
            eta = (1 - std::pow(2.0 / numItems, 1 - theta)) / (1 - zeta2Theta / zetan);
        }
        double u = dist(rng);
        double uz = u * zetan;
        if (uz < 1.0) {
            return min;
        }

        if (uz < 1.0 + std::pow(0.5, theta)) {
            return min + 1;
        }

        uint64_t ret = min + (long) ((numItems) *std::pow(eta * u - eta + 1, alpha));
        return ret;
    }

  private:
    /**
     * @brief this function calculates the zeta constant needed for the distribution
     * @param st
     * @param n
     * @param thetaVal
     * @param initialSum
     * @return newly computed zeta constant
     */
    double zeta(uint64_t st, uint64_t n, double thetaVal, double initialSum) {
        countForZeta = n;
        return computeZeta(st, n, thetaVal, initialSum);
    }

    /**
     * @brief Computes the zeta function by iterating over all numbers in the range of st .. n
     * @param st
     * @param n
     * @param theta
     * @param initialSum
     * @return newly computed zeta function
     */
    static double computeZeta(uint64_t st, uint64_t n, double theta, double initialSum) {
        double sum = initialSum;
        for (auto i = st; i < n; i++) {
            sum += 1 / (std::pow(i + 1, theta));
        }
        return sum;
    }

  private:
    uint64_t numItems;
    uint64_t min;
    double zipfianConstant;
    double alpha, zetan, eta, theta, zeta2Theta;
    uint64_t countForZeta;
    std::uniform_real_distribution<double> dist;
};
#endif// NES_BENCHMARK_INCLUDE_UTIL_ZIPFIANGENERATOR_HPP_
