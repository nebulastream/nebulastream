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
    Repository(int blockSize);
    void add(double value);
    void add(double value, bool isTested);
    double get(int index);
    void addAt(int index, double value);
    int getSize();
    void removeAll();

  private:
    std::vector<Block> listOfBlocks;
    const int blockSize;
    int indexOfLastBlock;
    int instanceCount;
    double total;

};

} // namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_STATISTICSCOLLECTOR_CHANGEDETECTORS_REPOSITORY_HPP_