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

class StatisticUtil {
  public:
    static std::vector<uint64_t> readFlattenedVectorFromCsvFile(const std::string& filename);

    static std::vector<std::vector<uint64_t>> read2DVectorFromCsvFile(const std::string& filename);
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_COMMON_INCLUDE_UTIL_STATISTICUTIL_HPP_
