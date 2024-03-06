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

#ifndef NES_NES_COMMON_INCLUDE_UTIL_STATISTICUTIL_HPP_
#define NES_NES_COMMON_INCLUDE_UTIL_STATISTICUTIL_HPP_

#include <string>
#include <vector>

namespace NES::Experimental::Statistics {

/**
 * @brief a class with basic utility functions for statistics
 */
class StatisticUtil {
  public:

    /**
     * @brief loads a csv file and writes the data to a 1D vector
     * @param filename the name of the csv file
     * @return a 1D vector containing the data from a csv file
     */
    static std::vector<uint64_t> readFlattenedVectorFromCsvFile(const std::string& filename);

    /**
     * @brief loads a csv file and writes the data to a 2D vector
     * @param filename the name of the csv file
     * @return a 1D vector containing the data from a csv file
     */
    static std::vector<std::vector<uint64_t>> read2DVectorFromCsvFile(const std::string& filename);

    /**
     * @param keySizeInBit the number of bits in the key
     * @param numfunctions the number of (independent) hash functions that are desired
     * @param seed the seed with which the random number generator will be set
     * @return a vector of seeds for H3
     */
    static std::vector<uint64_t> createH3Seeds(uint32_t keySizeInBit, uint64_t numfunctions, uint64_t seed = 42);
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_COMMON_INCLUDE_UTIL_STATISTICUTIL_HPP_
