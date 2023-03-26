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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_STATISTICSCOLLECTOR_CHANGEDETECTORS_RESERVOIR_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_STATISTICSCOLLECTOR_CHANGEDETECTORS_RESERVOIR_HPP_

#include <Execution/StatisticsCollector/ChangeDetectors/SeqDrift2/Repository.hpp>
#include <vector>

namespace NES::Runtime::Execution {

class Reservoir{
  public:
    Reservoir(int sampleSize, int blockSize);
    void addElement(double value);
    double get(int index);
    double getSampleMean() const;
    int getSize() const;
    double getTotal() const;
    void setSampleSize(int newSampleSize);
    void clear();
    void copy(Reservoir& source);

  private:
    int size;
    double total;
    //const int blockSize;
    int instanceCount;
    int sampleSize; // Max size
    Repository repository;

};
} // namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_STATISTICSCOLLECTOR_CHANGEDETECTORS_RESERVOIR_HPP_