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

#ifndef NES_MICROBENCHMARKRESULT_HPP
#define NES_MICROBENCHMARKRESULT_HPP

#include <string>
#include <unordered_map>

namespace NES::ASP::Benchmarking {

class MicroBenchmarkResult {
  public:
    explicit MicroBenchmarkResult(double throughput, double accuracy);

    void addToParams(const std::string& paramKey, const std::string& paramValue);

    void setThroughput(double throughput);

    void setAccuracy(double accuracy);

    /**
     * @brief Creates a header from the params that is deterministic
     * @return Header as a string with comma separated values
     */
    std::string getHeaderAsCsv();

    /**
     * @brief Creates a row from the params that is deterministic and corresponds with the header
     * @return Row as a string with comma separated values
     */
    std::string getRowAsCsv();

  private:
    const std::string THROUGHPUT = "throughput";
    const std::string ACCURACY = "accuracy";
    std::unordered_map<std::string, std::string> params;
};

} // namespace NES::ASP::Benchmarking

#endif//NES_MICROBENCHMARKRESULT_HPP
