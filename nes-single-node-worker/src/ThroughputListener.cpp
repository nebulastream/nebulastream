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

#include <map>
#include <unordered_map>
#include <utility>
#include <SliceStore/SliceAssigner.hpp>
#include <Util/Overloaded.hpp>
#include <Util/ThreadNaming.hpp>
#include <ThroughputListener.hpp>

namespace NES
{

namespace
{

Timestamp convertToTimeStamp(const ChronoClock::time_point timePoint)
{
    const auto milliSecondsSinceEpoch = std::chrono::duration_cast<std::chrono::milliseconds>(timePoint.time_since_epoch()).count();
    INVARIANT(milliSecondsSinceEpoch > 0, "milliSecondsSinceEpoch should be larger than 0 but are {}", milliSecondsSinceEpoch);
    return Timestamp(milliSecondsSinceEpoch);
}

std::chrono::system_clock::time_point convertToSystemClock(const std::chrono::high_resolution_clock::time_point& highResTimePoint)
{
    const auto durationSinceEpoch = highResTimePoint.time_since_epoch();
    return std::chrono::system_clock::time_point(std::chrono::duration_cast<std::chrono::system_clock::duration>(durationSinceEpoch));
}

void threadRoutine(
    std::ofstream& file,
    const std::stop_token& token,
    const Timestamp::Underlying timeIntervalInMilliSeconds,
    folly::MPMCQueue<Event>& events,
    const std::shared_ptr<Memory::BufferManager>& bufferManager,
    const std::function<void(std::ofstream&, const ThroughputListener::CallBackParams&)>& callBack)
{
    PRECONDITION(callBack != nullptr, "Call Back is null");

    setThreadName("ThroughputCalculator");

    /// All variables necessary for performing a simple tumbling window average aggregation per query id
    const SliceAssigner sliceAssigner(timeIntervalInMilliSeconds, timeIntervalInMilliSeconds);
    struct ThroughputWindow
    {
        explicit ThroughputWindow()
            : startTime(Timestamp::INVALID_VALUE), endTime(Timestamp::INVALID_VALUE), bytesProcessed(0), tuplesProcessed(0)
        {
        }
        Timestamp startTime;
        Timestamp endTime;
        uint64_t bytesProcessed;
        uint64_t tuplesProcessed;

        bool operator<(const ThroughputWindow& other) const { return endTime < other.endTime; }
    };
    struct TaskIntermediateStore
    {
        TaskIntermediateStore(
            QueryId queryId,
            Timestamp startTime,
            const uint64_t bytes,
            const uint64_t numberOfTuples,
            ChronoClock::time_point startTimePoint)
            : queryId(std::move(queryId))
            , startTime(std::move(startTime))
            , bytes(bytes)
            , numberOfTuples(numberOfTuples)
            , startTimePoint(std::move(startTimePoint))
        {
        }
        explicit TaskIntermediateStore() : queryId(INVALID_QUERY_ID), startTime(Timestamp::INVALID_VALUE), bytes(0), numberOfTuples(0) { }
        QueryId queryId;
        Timestamp startTime;
        uint64_t bytes;
        uint64_t numberOfTuples;
        ChronoClock::time_point startTimePoint;
    };

    /// We need to have for each query id windows that store the number of tuples processed in one.
    /// For faster access, we store the window with its end time as the key in a hash map.
    std::unordered_map<QueryId, std::map<Timestamp, ThroughputWindow>> queryIdToThroughputWindowMap;

    /// It might happen that a task takes longer than a timeInterval. Therefore, we might need to distribute the number of tuples
    /// across multiple intervals / slices. To accomplish this, we store each TaskExecutionStart in a hash map.
    std::unordered_map<TaskId, TaskIntermediateStore> taskIdToTaskIntermediateStoreMap;

    auto startTimeNoEventsTriggered = std::chrono::high_resolution_clock::now();
    while (!token.stop_requested())
    {
        Event event = QueryStart{WorkerThreadId(0), QueryId(0)}; /// Will be overwritten

        /// Check if there are any events to handle
        if (!events.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(100), event))
        {
            std::cout << "noEventRead\n";
            /// Check if we have see no event for more than timeIntervalInMilliSeconds. If this is the case, we invoke the callback with a throughput of 0
            const auto curTimePoint = std::chrono::high_resolution_clock::now();
            const auto noEventSeenFor
                = std::chrono::duration_cast<std::chrono::milliseconds>(curTimePoint - startTimeNoEventsTriggered).count();
            if (noEventSeenFor > static_cast<long>(timeIntervalInMilliSeconds))
            {
                std::cout << "noEventSeen\n";
                /// We call the callback and set the throughput to 0 and create a window of the current time
                const auto curTime = convertToTimeStamp(convertToSystemClock(curTimePoint));
                const auto windowStart = sliceAssigner.getSliceStartTs(curTime);
                const auto windowEnd = sliceAssigner.getSliceEndTs(curTime);
                const ThroughputListener::CallBackParams callbackParams
                    = {.queryId = INVALID_QUERY_ID,
                       .windowStart = windowStart,
                       .windowEnd = windowEnd,
                       .throughputInBytesPerSec = 0,
                       .throughputInTuplesPerSec = 0,
                       .memoryConsumption = 0};
                callBack(file, callbackParams);
                startTimeNoEventsTriggered = curTimePoint;
            }
            continue;
        }
        std::visit(
            Overloaded{
                [&](const TaskExecutionStart& taskStartEvent)
                {
                    const auto taskId = taskStartEvent.taskId;
                    const auto queryId = taskStartEvent.queryId;
                    const auto bytes = taskStartEvent.bytesInTupleBuffer;
                    const auto numberOfTuples = taskStartEvent.numberOfTuples;
                    const auto startTime = convertToTimeStamp(taskStartEvent.timestamp);
                    const auto intermediateStoreItem
                        = TaskIntermediateStore(queryId, startTime, bytes, numberOfTuples, taskStartEvent.timestamp);
                    INVARIANT(
                        not taskIdToTaskIntermediateStoreMap.contains(taskId),
                        "TaskId {} was already presented with start time {} and new start time {}",
                        taskId,
                        taskIdToTaskIntermediateStoreMap.at(taskId).startTime,
                        startTime);
                    taskIdToTaskIntermediateStoreMap[taskId] = intermediateStoreItem;
                },
                [&](const TaskEmit& taskEmit)
                {
                    /// We define the throughput to be the performance of the formatting steps, i.e., the throughput of emitting work from the formatting
                    /// If this task did not belong to a formatting task, we ignore it and return
                    if (not taskEmit.formattingTask)
                    {
                        return;
                    }
                    const auto numberOfTuples = taskEmit.numberOfTuples;
                    const auto bytes = taskEmit.bytesInTupleBuffer;
                    const auto queryId = taskEmit.queryId;
                    const auto endTime = convertToTimeStamp(taskEmit.timestamp);
                    const auto windowStart = sliceAssigner.getSliceStartTs(endTime);
                    const auto windowEnd = sliceAssigner.getSliceEndTs(endTime);
                    queryIdToThroughputWindowMap[queryId][windowEnd].bytesProcessed += bytes;
                    queryIdToThroughputWindowMap[queryId][windowEnd].tuplesProcessed += numberOfTuples;
                    queryIdToThroughputWindowMap[queryId][windowEnd].startTime = windowStart;
                    queryIdToThroughputWindowMap[queryId][windowEnd].endTime = windowEnd;

                    /// Now we need to check if we can emit / calculate a throughput. We assume that taskStopEvent.timestamp is increasing
                    for (auto& [queryId, endTimeAndThroughputWindow] : queryIdToThroughputWindowMap)
                    {
                        for (auto it = endTimeAndThroughputWindow.begin(); it != endTimeAndThroughputWindow.end();)
                        {
                            const auto& curWindowEnd = it->first;
                            const auto& [startTime, endTime, bytesProcessed, tuplesProcessed] = it->second;
                            if (curWindowEnd + timeIntervalInMilliSeconds >= windowEnd)
                            {
                                /// As the windows are sorted by their end time, we can break here
                                break;
                            }

                            /// Calculating the throughput over this window and letting the callback know that a new throughput has been calculated
                            const auto durationInMilliseconds = (endTime - startTime).getRawValue();
                            const auto throughputInBytesPerSec = bytesProcessed / (durationInMilliseconds / 1000.0);
                            const auto throughputInTuplesPerSec = tuplesProcessed / (durationInMilliseconds / 1000.0);
                            const auto totalSizeOfUsedBuffers
                                = (bufferManager->getNumOfPooledBuffers() - bufferManager->getAvailableBuffers())
                                    * bufferManager->getBufferSize()
                                + bufferManager->getTotalSizeOfUnpooledBufferChunks();
                            const ThroughputListener::CallBackParams callbackParams
                                = {.queryId = queryId,
                                   .windowStart = startTime,
                                   .windowEnd = endTime,
                                   .throughputInBytesPerSec = throughputInBytesPerSec,
                                   .throughputInTuplesPerSec = throughputInTuplesPerSec,
                                   .memoryConsumption = totalSizeOfUsedBuffers};
                            callBack(file, callbackParams);

                            /// Removing the window, as we do not need it anymore
                            it = endTimeAndThroughputWindow.erase(it);

                            /// Resetting the startTimeNoEventsTriggered
                            startTimeNoEventsTriggered = std::chrono::high_resolution_clock::now();
                        }
                    }
                },
                [](auto) {}},
            event);
    }
}
}

void ThroughputListener::onEvent(Event event)
{
    events.writeIfNotFull(std::visit([]<typename T>(T&& arg) { return Event(std::forward<T>(arg)); }, std::move(event)));
}

ThroughputListener::ThroughputListener(
    const std::filesystem::path& path,
    const Timestamp::Underlying timeIntervalInMilliSeconds,
    const std::function<void(std::ofstream&, const CallBackParams&)>& callBack)
    : file(path, std::ios::out | std::ios::app)
    , timeIntervalInMilliSeconds(timeIntervalInMilliSeconds)
    , callBack(callBack)
    , calculateThread([this](const std::stop_token& stopToken)
                      { threadRoutine(file, stopToken, this->timeIntervalInMilliSeconds, events, bufferManager, this->callBack); })
{
}

void ThroughputListener::setBufferManager(std::shared_ptr<Memory::BufferManager> bufferManager)
{
    this->bufferManager = std::move(bufferManager);
}

}
