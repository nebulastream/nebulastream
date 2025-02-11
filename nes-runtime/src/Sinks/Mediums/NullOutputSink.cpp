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

#include <Runtime/QueryManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/MultiOriginWatermarkProcessor.hpp>
#include <Sinks/Mediums/NullOutputSink.hpp>
#include <Util/Logger/Logger.hpp>
#include <sstream>
#include <string>
#include <iomanip>
#include <sys/time.h>

namespace NES {
NullOutputSink::NullOutputSink(Runtime::NodeEnginePtr nodeEngine,
                               uint32_t numOfProducers,
                               SharedQueryId sharedQueryId,
                               DecomposedQueryId decomposedQueryId,
                               DecomposedQueryPlanVersion decomposedQueryVersion,
                               FaultToleranceType faultToleranceType,
                               uint64_t numberOfOrigins)
    : SinkMedium(nullptr,
                 std::move(nodeEngine),
                 numOfProducers,
                 sharedQueryId,
                 decomposedQueryId,
                 decomposedQueryVersion,
                 faultToleranceType,
                 numberOfOrigins,
                 std::make_unique<Windowing::MultiOriginWatermarkProcessor>(numberOfOrigins)) {
    if (faultToleranceType == FaultToleranceType::M) {
        duplicateDetectionCallback = [this](Runtime::TupleBuffer& inputBuffer){
            return watermarkProcessor->isDuplicate(inputBuffer.getSequenceNumber(), inputBuffer.getOriginId());
        };
    }
    else {
        duplicateDetectionCallback = [](Runtime::TupleBuffer&){
            return false;
        };
    }
    statisticsFile.open("latency.csv", std::ios::out);
    statisticsFile << "time,latency\n";
}

NullOutputSink::~NullOutputSink() = default;

SinkMediumTypes NullOutputSink::getSinkMediumType() { return SinkMediumTypes::NULL_SINK; }

bool NullOutputSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    std::unique_lock lock(writeMutex);
    if (!duplicateDetectionCallback(inputBuffer)) {
        updateWatermarkCallback(inputBuffer);
        // std::string str = "";
    // auto now = std::chrono::system_clock::now();
    // auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    // auto epoch = now_ms.time_since_epoch();
    // auto value = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);
    // auto ts = std::chrono::system_clock::now();
    // timeval curTime;
    // gettimeofday(&curTime, NULL);
    // int milli = curTime.tv_usec / 1000;
    // auto timeNow = std::chrono::system_clock::to_time_t(ts);
    // char buffer[26];
    // int millisec;
    // struct tm* tm_info;
    // struct timeval tv;
    //
    // gettimeofday(&tv, NULL);
    //
    // millisec = lrint(tv.tv_usec / 1000.0);// Round to nearest millisec
    // if (millisec >= 1000) {               // Allow for rounding up to nearest second
    //     millisec -= 1000;
    //     tv.tv_sec++;
    // }
    //
    // tm_info = localtime(&tv.tv_sec);
    //
    // strftime(buffer, 26, "%Y:%m:%d %H:%M:%S", tm_info);
    // statisticsFile << std::put_time(std::localtime(&timeNow), "%Y-%m-%d %H:%M:%S") << ".";
    // if (millisec < 10)
    //     statisticsFile << "00" << millisec << ",";
    // else if (millisec < 100)
    //     statisticsFile << "0" << millisec << ",";
    // else
    //     statisticsFile << millisec << ",";
    // unsigned lastDelimPos = str.find_last_of("|");
    // unsigned firstDelimPos = str.find("|");
    // unsigned nextDelimPosition = firstDelimPos;
    // while (nextDelimPosition != lastDelimPos) {
    //     std::cout << "firstDelimPos" << firstDelimPos;
    //     firstDelimPos = nextDelimPosition;
    //     nextDelimPosition = str.find_first_of("|", nextDelimPosition + 2);
    //     std::cout << "nextDelimPosition" << nextDelimPosition;
    // }
    // if (!str.empty()) {
    //     std::string creationTimestamp = str.substr(firstDelimPos + 1, nextDelimPosition - firstDelimPos - 1);
    //     std::string::const_iterator it = creationTimestamp.begin();
    //     while (it != creationTimestamp.end() && std::isdigit(*it)) ++it;
    //     if (it == creationTimestamp.end()) {
    //         statisticsFile << value.count() - std::atoll(creationTimestamp.c_str()) << "\n";
    //         statisticsFile.flush();
    //     }
    // }
    // else {
    //     statisticsFile << value.count() - inputBuffer.getCreationTimestampInMS() << "\n";
    // }
    }

    return true;
}

std::string NullOutputSink::toString() const {
    std::stringstream ss;
    ss << "NULL_SINK(";
    ss << ")";
    return ss.str();
}

void NullOutputSink::setup() {

}

void NullOutputSink::shutdown() {
}
}// namespace NES
