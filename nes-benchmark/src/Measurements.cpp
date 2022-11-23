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
#include <sstream>

namespace NES::Benchmark::Measurements {

std::vector<std::string> Measurements::getMeasurementsAsCSV(size_t schemaSizeInByte) {
    std::vector<std::string> vecCsvStrings;
    for (size_t measurementIdx = 0; measurementIdx < allTimeStamps.size() - 1; ++measurementIdx) {
        std::stringstream measurementsCsv;
        measurementsCsv << allTimeStamps[measurementIdx];
        measurementsCsv << "," << allProcessedTasks[measurementIdx];
        measurementsCsv << "," << allProcessedBuffers[measurementIdx];
        measurementsCsv << "," << allProcessedTuples[measurementIdx];
        measurementsCsv << "," << allLatencySum[measurementIdx];
        measurementsCsv << "," << allQueueSizeSums[measurementIdx];
        measurementsCsv << "," << allAvailGlobalBufferSum[measurementIdx];
        measurementsCsv << "," << allAvailFixedBufferSum[measurementIdx];

        double timeDeltaSeconds = (allTimeStamps[measurementIdx + 1] - allTimeStamps[measurementIdx]) / 1000;
        uint64_t tuplesPerSecond =
            (allProcessedTuples[measurementIdx + 1] - allProcessedTuples[measurementIdx]) / (timeDeltaSeconds);
        uint64_t tasksPerSecond =
            (allProcessedTasks[measurementIdx + 1] - allProcessedTasks[measurementIdx]) / (timeDeltaSeconds);
        uint64_t bufferPerSecond =
            (allProcessedBuffers[measurementIdx + 1] - allProcessedBuffers[measurementIdx]) / (timeDeltaSeconds);
        uint64_t mebiBPerSecond = (tuplesPerSecond * schemaSizeInByte) / (1024 * 1024);

        measurementsCsv << "," << tuplesPerSecond << "," << tasksPerSecond;
        measurementsCsv << "," << bufferPerSecond << "," << mebiBPerSecond;

        vecCsvStrings.push_back(measurementsCsv.str());
    }

    return vecCsvStrings;
}
}// namespace NES::Benchmark::Measurements