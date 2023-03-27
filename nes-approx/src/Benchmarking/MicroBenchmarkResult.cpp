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

#include <Benchmarking/MicroBenchmarkResult.hpp>

namespace NES::ASP::Benchmarking {

void MicroBenchmarkResult::setThroughput(double throughput) { params[THROUGHPUT] = std::to_string(throughput); }

void MicroBenchmarkResult::setAccuracy(double accuracy) {  params[ACCURACY] = std::to_string(accuracy); }

void MicroBenchmarkResult::addToParams(const std::string& paramKey, const std::string& paramValue) {
    params[paramKey] = paramValue;
}

MicroBenchmarkResult::MicroBenchmarkResult(double throughput, double accuracy)  {
    this->setAccuracy(accuracy);
    this->setThroughput(throughput);
}

} // namespace NES::ASP::Benchmarking