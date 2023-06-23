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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_STATISTICSCOLLECTOR_CHANGEDETECTORS_REPOSITORY_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_STATISTICSCOLLECTOR_CHANGEDETECTORS_REPOSITORY_HPP_

#include <Execution/StatisticsCollector/ChangeDetectors/SeqDrift2/Block.hpp>
#include <vector>

namespace NES::Runtime::Execution {

class Repository{
  public:
    Repository(int64_t blockSize);
    void add(double value);
    double get(int64_t index);
    void addAt(int64_t index, double value);
    int64_t getSize() const;
    void removeAll();

  private:
    std::vector<Block> listOfBlocks;
    const int64_t blockSize;
    int64_t indexOfLastBlock;
    int64_t instanceCount;
    double total;

};

} // namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_STATISTICSCOLLECTOR_CHANGEDETECTORS_REPOSITORY_HPP_