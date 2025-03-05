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

#pragma once

#include <cstdint>
#include <queue>
#include <folly/MPMCQueue.h>

namespace NES::Runtime {
class PipelineStatistics {
public:
  explicit PipelineStatistics(const size_t windowSizeRollingAverage)
    : sumThroughput(0), sumLatency(0), sumNumberOfTuples(0), windowSizeRollingAverage(windowSizeRollingAverage),
      nextNumberOfTuplesPerTask(1) {
  }

  explicit PipelineStatistics() : PipelineStatistics(10) {
  }

  struct TaskStatistics {
    double throughput;
    std::chrono::microseconds latency;
    uint64_t numberOfTuples;
  };

  /// Adding the statistics of a task to the pipeline statistics and calculating the rolling average
  void updateTaskStatistics(const TaskStatistics &taskStatistic);
  [[nodiscard]] double getAverageThroughput() const;
  [[nodiscard]] std::chrono::microseconds getAverageLatency() const;
  [[nodiscard]] uint64_t getAverageNumberOfTuples() const;
  [[nodiscard]] uint64_t getNextNumberOfTuplesPerTask() const;

  /// Changing the number of tuples per task by a factor. The minimum number of tuples per task is 1, regardless of the factor
  void changeNextNumberOfTuplesPerTask(double factor);

private:
  double sumThroughput;
  std::chrono::microseconds sumLatency;
  uint64_t sumNumberOfTuples;
  std::queue<TaskStatistics> storedTaskStatistics;
  size_t windowSizeRollingAverage;
  uint64_t nextNumberOfTuplesPerTask;
};
}
