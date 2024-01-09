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
#include <Sinks/Mediums/NullOutputSink.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <sstream>
#include <string>

namespace NES {
NullOutputSink::NullOutputSink(Runtime::NodeEnginePtr nodeEngine,
                               uint32_t numOfProducers,
                               QueryId queryId,
                               QuerySubPlanId querySubPlanId,
                               FaultToleranceType::Value faultToleranceType,
                               uint64_t numberOfOrigins)
    : SinkMedium(nullptr,
                 std::move(nodeEngine),
                 numOfProducers,
                 queryId,
                 querySubPlanId,
                 faultToleranceType,
                 numberOfOrigins,
                 std::make_unique<Windowing::MultiOriginWatermarkProcessor>(numberOfOrigins)) {
    //        trimmingThread = std::make_shared<std::thread>(([this]() {
    //            while(this->nodeEngine->getQueryStatus(this->queryId) == Runtime::Execution::ExecutableQueryPlanStatus::Running) {
    //                std::this_thread::sleep_for(5ms);
    //                auto timestamp = this->watermarkProcessor->getCurrentWatermark();
    //                if(timestamp) {
    //                    notifyEpochTermination(timestamp);
    //                }
    //            }
    //        }));
        statisticsFile.open("latency.csv", std::ios::out);
        statisticsFile << "time,latency\n";
}

NullOutputSink::~NullOutputSink() {
        statisticsFile.flush();
        statisticsFile.close();
};

SinkMediumTypes NullOutputSink::getSinkMediumType() { return NULL_SINK; }

bool NullOutputSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext&) {
    std::unique_lock lock(writeMutex);
    std::string str = "";
    NES_DEBUG("Print tuple" << inputBuffer.getSequenceNumber());
    auto now = std::chrono::system_clock::now();
    auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    auto epoch = now_ms.time_since_epoch();
    auto value = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);
    auto ts = std::chrono::system_clock::now();
    timeval curTime;
    gettimeofday(&curTime, NULL);
    int milli = curTime.tv_usec / 1000;
    auto timeNow = std::chrono::system_clock::to_time_t(ts);
    char buffer[26];
    int millisec;
    struct tm* tm_info;
    struct timeval tv;

    gettimeofday(&tv, NULL);

    millisec = lrint(tv.tv_usec / 1000.0);// Round to nearest millisec
    if (millisec >= 1000) {               // Allow for rounding up to nearest second
        millisec -= 1000;
        tv.tv_sec++;
    }

    tm_info = localtime(&tv.tv_sec);

    strftime(buffer, 26, "%Y:%m:%d %H:%M:%S", tm_info);
    statisticsFile << std::put_time(std::localtime(&timeNow), "%Y-%m-%d %H:%M:%S") << ".";
    if (millisec < 10)
        statisticsFile << "00" << millisec << ",";
    else if (millisec < 100)
        statisticsFile << "0" << millisec << ",";
    else
        statisticsFile << millisec << ",";
    unsigned lastDelimPos = str.find_last_of("|");
    unsigned firstDelimPos = str.find("|");
    unsigned nextDelimPosition = firstDelimPos;
    while (nextDelimPosition != lastDelimPos) {
        std::cout << "firstDelimPos" << firstDelimPos;
        firstDelimPos = nextDelimPosition;
        nextDelimPosition = str.find_first_of("|", nextDelimPosition + 2);
        std::cout << "nextDelimPosition" << nextDelimPosition;
    }
    if (!str.empty()) {
        std::string creationTimestamp = str.substr(firstDelimPos + 1, nextDelimPosition - firstDelimPos - 1);
        NES_TRACE("PrintSink::Converted timestamp " << creationTimestamp);
        std::string::const_iterator it = creationTimestamp.begin();
        while (it != creationTimestamp.end() && std::isdigit(*it)) ++it;
        if (it == creationTimestamp.end()) {
            statisticsFile << value.count() - std::atoll(creationTimestamp.c_str()) << "\n";
            statisticsFile.flush();
        }
    }
    else {
        statisticsFile << value.count() - inputBuffer.getCreationTimestampInMS() << "\n";
    }
    updateWatermarkCallback(inputBuffer);
    return true;
}

std::string NullOutputSink::toString() const {
    std::stringstream ss;
    ss << "NULL_SINK(";
    ss << ")";
    return ss.str();
}

void NullOutputSink::setup() {
    // currently not required
}
void NullOutputSink::shutdown() {
    // currently not required
}

}// namespace NES
