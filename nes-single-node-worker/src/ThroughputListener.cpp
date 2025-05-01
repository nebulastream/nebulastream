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

Timestamp convertToTimeStamp(const ChronoClock::time_point timePoint)
{
    const unsigned long milliSecondsSinceEpoch
        = std::chrono::time_point_cast<std::chrono::milliseconds>(timePoint).time_since_epoch().count();
    INVARIANT(milliSecondsSinceEpoch > 0, "milliSecondsSinceEpoch should be larger than 0 but are {}", milliSecondsSinceEpoch);
    return Timestamp(milliSecondsSinceEpoch);
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
        TaskIntermediateStore(QueryId queryId, Timestamp startTime, const uint64_t bytes, ChronoClock::time_point startTimePoint)
            : queryId(std::move(queryId)), startTime(std::move(startTime)), bytes(bytes), startTimePoint(std::move(startTimePoint))
        {
        }
        explicit TaskIntermediateStore() : queryId(INVALID_QUERY_ID), startTime(Timestamp::INVALID_VALUE), bytes(0) { }
        QueryId queryId;
        Timestamp startTime;
        uint64_t bytes;
        ChronoClock::time_point startTimePoint;
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
                    const auto intermediateStoreItem = TaskIntermediateStore(queryId, startTime, bytes, taskStartEvent.timestamp);
                    INVARIANT(
                        not taskIdToTaskIntermediateStoreMap.contains(taskId),
                        "TaskId {} was already presented with start time {} and new start time {}",
                        taskId,
                        taskIdToTaskIntermediateStoreMap.at(taskId).startTime,
                        startTime);
                    taskIdToTaskIntermediateStoreMap[taskId] = intermediateStoreItem;
                },
                [&](const TaskExecutionComplete& taskStopEvent)
                {
                    const auto taskId = taskStopEvent.taskId;
                    const auto queryId = taskStopEvent.queryId;
                    const auto endTime = convertToTimeStamp(taskStopEvent.timestamp);
                    if (endTime.getRawValue() == 3384)
                    {
                        NES_INFO("asdasd");
                    }
                    if (taskIdToTaskIntermediateStoreMap.contains(taskId))
                    {
                        std::cout << fmt::format("TaskId {} must be in taskIdToTaskIntermediateStoreMap but it is not", taskId) << std::endl;
                    }


                    const auto startTime = taskIdToTaskIntermediateStoreMap[taskId].startTime;
                    const auto startTimePoint = taskIdToTaskIntermediateStoreMap[taskId].startTimePoint;
                    const auto bytes = taskIdToTaskIntermediateStoreMap[taskId].bytes;
                    taskIdToTaskIntermediateStoreMap.erase(taskId);

                    /// We need to check if the task started and completed in the same interval
                    const auto windowStartForEndTime = sliceAssigner.getSliceStartTs(startTime);
                    const auto windowEndForEndTime = sliceAssigner.getSliceEndTs(endTime);

                    /// Task started and completed in the same window.
                    /// Add the number of tuples to the window of the query
                    queryIdToThroughputWindowMap[queryId][windowEndForEndTime].bytesProcessed += bytes;
                    queryIdToThroughputWindowMap[queryId][windowEndForEndTime].startTime = windowStartForEndTime;
                    queryIdToThroughputWindowMap[queryId][windowEndForEndTime].endTime = windowEndForEndTime;



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

                            /// Calculating the throughput over this window and letting the callback know that a new throughput has been calculated
                            const auto durationInMilliseconds = (endTime - startTime).getRawValue();
                            const auto throughputInBytesPerSec = bytesProcessed / (durationInMilliseconds / 1000.0);
                            if (startTime.getRawValue() == 18446744073709550000UL)
                            {
                                NES_INFO("blab");
                            }
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