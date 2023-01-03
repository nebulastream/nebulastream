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

#include <Measurements.hpp>
#include <iostream>
#include <sstream>

namespace NES::Benchmark::Measurements {

std::vector<std::string> Measurements::getMeasurementsAsCSV(size_t schemaSizeInByte, size_t numberOfQueries) {
    double maxTuplesPerSecond = 0;
    std::vector<std::string> vecCsvStrings;
    for (size_t measurementIdx = 0; measurementIdx < timestamps.size() - 1; ++measurementIdx) {
        std::stringstream measurementsCsv;
        auto currentTs = timestamps[measurementIdx];
        measurementsCsv << currentTs;
        measurementsCsv << "," << allProcessedTasks[currentTs];
        measurementsCsv << "," << allProcessedBuffers[currentTs];
        measurementsCsv << "," << allProcessedTuples[currentTs];
        measurementsCsv << "," << allLatencySum[currentTs];
        measurementsCsv << "," << allQueueSizeSums[currentTs] / numberOfQueries;
        measurementsCsv << "," << allAvailGlobalBufferSum[currentTs] / numberOfQueries;
        measurementsCsv << "," << allAvailFixedBufferSum[currentTs] / numberOfQueries;

        double timeDeltaSeconds = (timestamps[measurementIdx + 1] - timestamps[measurementIdx]);
        double tuplesPerSecond =
            (allProcessedTuples[timestamps[measurementIdx + 1]] - allProcessedTuples[currentTs]) / (timeDeltaSeconds);
        double tasksPerSecond =
            (allProcessedTasks[timestamps[measurementIdx + 1]] - allProcessedTasks[currentTs]) / (timeDeltaSeconds);
        maxTuplesPerSecond = std::max(maxTuplesPerSecond, tuplesPerSecond);
        double bufferPerSecond =
            (allProcessedBuffers[timestamps[measurementIdx + 1]] - allProcessedBuffers[currentTs]) / (timeDeltaSeconds);
        double mebiBPerSecond = std::max(0.0, (tuplesPerSecond * schemaSizeInByte) / (1024 * 1024));

        measurementsCsv << "," << tuplesPerSecond << "," << tasksPerSecond;
        measurementsCsv << "," << bufferPerSecond << "," << mebiBPerSecond;

        vecCsvStrings.push_back(measurementsCsv.str());
    }

    return vecCsvStrings;
}
}// namespace NES::Benchmark::Measurements