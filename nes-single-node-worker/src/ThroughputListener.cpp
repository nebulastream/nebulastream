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
#include <Execution/Operators/SliceStore/SliceAssigner.hpp>
#include <Util/Overloaded.hpp>
#include <Util/ThreadNaming.hpp>
#include <ThroughputListener.hpp>

namespace NES::Runtime
{

namespace
{

Timestamp convertToTimeStamp(ChronoClock::time_point timePoint)
{
    const auto durationSinceEpoch = timePoint.time_since_epoch();
    const auto milliSecondsSinceEpoch = std::chrono::duration_cast<std::chrono::milliseconds>(durationSinceEpoch);
    return Timestamp(milliSecondsSinceEpoch.count());
}

void threadRoutine(
    const std::stop_token& token,
    const Timestamp::Underlying timeIntervalInMilliSeconds,
    folly::MPMCQueue<Event>& events,
    const std::function<void(const QueryId&, const Timestamp&, const Timestamp&, double)>& callBack)
{
    PRECONDITION(callBack != nullptr, "Call Back is null");

    setThreadName("ThroughputCalculator");

    /// All variables necessary for performing a simple tumbling window average aggregation per query id
    const Execution::Operators::SliceAssigner sliceAssigner(timeIntervalInMilliSeconds, timeIntervalInMilliSeconds);
    struct ThroughputWindow
    {
        explicit ThroughputWindow() : startTime(Timestamp::INVALID_VALUE), endTime(Timestamp::INVALID_VALUE), bytesProcessed(0) { }
        Timestamp startTime;
        Timestamp endTime;
        uint64_t bytesProcessed;

        bool operator<(const ThroughputWindow& other) const { return endTime < other.endTime; }
    };
    struct TaskIntermediateStore
    {
        TaskIntermediateStore(QueryId queryId, Timestamp startTime, const uint64_t bytes)
            : queryId(std::move(queryId)), startTime(std::move(startTime)), bytes(bytes)
        {
        }
        explicit TaskIntermediateStore() : queryId(INVALID_QUERY_ID), startTime(Timestamp::INVALID_VALUE), bytes(0) { }
        QueryId queryId;
        Timestamp startTime;
        uint64_t bytes;
    };

    /// We need to have for each query id windows that store the number of tuples processed in one.
    /// For faster access, we store the window with its end time as the key in a hash map.
    std::unordered_map<QueryId, std::map<Timestamp, ThroughputWindow>> queryIdToThroughputWindowMap;

    /// It might happen that a task takes longer than a timeInterval. Therefore, we might need to distribute the number of tuples
    /// across multiple intervals / slices. To accomplish this, we store each TaskExecutionStart in a hash map.
    std::unordered_map<TaskId, TaskIntermediateStore> taskIdToTaskIntermediateStoreMap;


    while (!token.stop_requested())
    {
        Event event = QueryStart{WorkerThreadId(0), QueryId(0)}; /// Will be overwritten

        if (!events.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(100), event))
        {
            continue;
        }
        std::visit(
            Overloaded{
                [&](const TaskExecutionStart& taskStartEvent)
                {
                    const auto taskId = taskStartEvent.taskId;
                    const auto queryId = taskStartEvent.queryId;
                    const auto bytes = taskStartEvent.bytesInTupleBuffer;
                    const auto startTime = convertToTimeStamp(taskStartEvent.timestamp);
                    const auto intermediateStoreItem = TaskIntermediateStore(queryId, startTime, bytes);
                    taskIdToTaskIntermediateStoreMap[taskId] = intermediateStoreItem;
                },
                [&](const TaskExecutionComplete& taskStopEvent)
                {
                    const auto taskId = taskStopEvent.taskId;
                    const auto queryId = taskStopEvent.queryId;
                    const auto endTime = convertToTimeStamp(taskStopEvent.timestamp);
                    const auto startTime = taskIdToTaskIntermediateStoreMap[taskId].startTime;
                    const auto bytes = taskIdToTaskIntermediateStoreMap[taskId].bytes;
                    taskIdToTaskIntermediateStoreMap.erase(taskId);

                    /// We need to check if the task started and completed in the same interval
                    const auto windowStartForStartTime = sliceAssigner.getSliceStartTs(startTime);
                    const auto windowEndForStartTime = sliceAssigner.getSliceEndTs(startTime);
                    const auto windowStartForEndTime = sliceAssigner.getSliceStartTs(endTime);
                    const auto windowEndForEndTime = sliceAssigner.getSliceEndTs(endTime);
                    const auto sameWindow = windowEndForStartTime == windowEndForEndTime;

                    // std::cout << fmt::format(
                    //     "sameWindow is {} for startTime {} and endTime {} with taskStopEvent.timestamp {}",
                    //     sameWindow,
                    //     startTime,
                    //     endTime,
                    //     taskStopEvent.timestamp)
                    //           << std::endl;

                    if (sameWindow)
                    {
                        /// Task started and completed in the same window.
                        /// Therefore, we can simply add the number of tuples to the window of the query
                        queryIdToThroughputWindowMap[queryId][windowEndForStartTime].bytesProcessed += bytes;
                        queryIdToThroughputWindowMap[queryId][windowEndForStartTime].startTime = windowStartForStartTime;
                        queryIdToThroughputWindowMap[queryId][windowEndForStartTime].endTime = windowEndForStartTime;

                        // std::cout << fmt::format(
                        //     "bytesProcessed for window {}-{} is {}",
                        //     queryIdToThroughputWindowMap[queryId][windowEndForStartTime].startTime,
                        //     queryIdToThroughputWindowMap[queryId][windowEndForStartTime].endTime,
                        //     queryIdToThroughputWindowMap[queryId][windowEndForStartTime].bytesProcessed)
                        //           << std::endl;
                    }
                    else
                    {
                        /// Task started and completed in different windows
                        /// For now, we simply say that half of the number of tuples have been processed in both windows
                        /// So 50% of tuples in windowEndForStartTime and 50% in windowEndForEndTime.
                        /// If number of tuples is even, windowEndForStartTime will get the additional tuple
                        const auto bytesFirst = static_cast<uint64_t>(std::ceil(bytes / 2.0));
                        const auto bytesLast = static_cast<uint64_t>(std::floor(bytes / 2.0));

                        queryIdToThroughputWindowMap[queryId][windowEndForStartTime].bytesProcessed += bytesFirst;
                        queryIdToThroughputWindowMap[queryId][windowEndForStartTime].startTime = windowStartForStartTime;
                        queryIdToThroughputWindowMap[queryId][windowEndForStartTime].endTime = windowEndForStartTime;
                        queryIdToThroughputWindowMap[queryId][windowEndForEndTime].bytesProcessed += bytesLast;
                        queryIdToThroughputWindowMap[queryId][windowEndForEndTime].startTime = windowStartForEndTime;
                        queryIdToThroughputWindowMap[queryId][windowEndForEndTime].endTime = windowEndForEndTime;
                    }


                    /// Now we need to check if we can emit / calculate a throughput. We assume that taskStopEvent.timestamp is increasing
                    for (auto& [queryId, endTimeAndThroughputWindow] : queryIdToThroughputWindowMap)
                    {
                        for (auto it = endTimeAndThroughputWindow.cbegin(); it != endTimeAndThroughputWindow.cend();)
                        {
                            const auto& curWindowEnd = it->first;
                            const auto& [startTime, endTime, bytesProcessed] = it->second;
                            if (curWindowEnd + timeIntervalInMilliSeconds >= windowEndForEndTime)
                            {
                                /// As the windows are sorted by their end time, we can break here
                                break;
                            }

                            /// Calculating the throughput over this window and calling the function
                            const auto durationInMilliseconds = (endTime - startTime).getRawValue();
                            const auto throughputInBytesPerSec = bytesProcessed / (durationInMilliseconds / 1000.0);
                            callBack(queryId, startTime, endTime, throughputInBytesPerSec);

                            /// Removing the window, as we do not need it anymore
                            it = endTimeAndThroughputWindow.erase(it);
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
    const Timestamp::Underlying timeIntervalInMilliSeconds,
    const std::function<void(const QueryId&, const Timestamp&, const Timestamp&, double)>& callBack)
    : timeIntervalInMilliSeconds(timeIntervalInMilliSeconds)
    , callBack(callBack)
    , calculateThread([this](const std::stop_token& stopToken)
                      { threadRoutine(stopToken, this->timeIntervalInMilliSeconds, events, this->callBack); })

{
}
}