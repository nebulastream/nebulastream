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
#include <map>

namespace NES::ASP::Benchmarking {

/**
 * @brief This class stores the result of a single microbenchmark run. It operators on a hashmap to be able to support custom params.
 */
class MicroBenchmarkResult {
  public:
    /**
     * @brief Constructor of a MicroBenchmarkResult object
     * @param throughput
     * @param accuracy
     */
    explicit MicroBenchmarkResult(double throughput, double accuracy);

    /**
     * @brief Adds the parameter and the value to this result, e.g., numberOfRetries = 2 --> addToParams("numberOfRetries", "2")
     * @param paramKey
     * @param paramValue
     */
    void addToParams(const std::string& paramKey, const std::string& paramValue);

    /**
     * @brief Sets the throughput of the result
     * @param throughput
     */
    void setThroughput(double throughput);

    /**
     * @brief Sets the accuracy of the result
     * @param accuracy
     */
    void setAccuracy(double accuracy);

    /**
     * @brief Creates a header from the params that is deterministic
     * @return Header as a string with comma separated values
     */
    const std::string getHeaderAsCsv() const;

    /**
     * @brief Creates a row from the params that is deterministic and corresponds with the header
     * @return Row as a string with comma separated values
     */
    const std::string getRowAsCsv() const;

    /**
     * @brief Getter for the params
     * @param paramString
     * @return Reference to params
     */
    std::string getParam(const std::string& paramString);

  private:
    const std::string THROUGHPUT = "throughput";
    const std::string ACCURACY = "accuracy";
    std::map<std::string, std::string> params;
};

} // namespace NES::ASP::Benchmarking

#endif//NES_MICROBENCHMARKRESULT_HPP
