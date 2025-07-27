#include <ReplayPrinter.hpp>
#include <chrono>
#include <iostream>
#include <thread>

int main()
{
    /// Create a replay printer that writes to a test directory
    auto replayPrinter = std::make_shared<NES::ReplayPrinter>("test_replay_data");
    
    /// Simulate some events for multiple queries
    auto now = std::chrono::system_clock::now();
    
    /// Query 1 events
    NES::SubmitQuerySystemEvent submitEvent1{NES::QueryId(1), "SELECT * FROM test1", now};
    replayPrinter->onEvent(submitEvent1);
    
    NES::StartQuerySystemEvent startEvent1{NES::QueryId(1), now + std::chrono::milliseconds(10)};
    replayPrinter->onEvent(startEvent1);
    
    NES::QueryStart queryStart1{NES::WorkerThreadId(1), NES::QueryId(1), now + std::chrono::milliseconds(20)};
    replayPrinter->onEvent(queryStart1);
    
    NES::PipelineStart pipelineStart1{NES::PipelineId(1), NES::QueryId(1), NES::WorkerThreadId(1), now + std::chrono::milliseconds(30)};
    replayPrinter->onEvent(pipelineStart1);
    
    NES::TaskExecutionStart taskStart1{NES::TaskId(1), NES::PipelineId(1), NES::QueryId(1), NES::WorkerThreadId(1), 100, NES::TupleData{}, now + std::chrono::milliseconds(40)};
    replayPrinter->onEvent(taskStart1);
    
    NES::TaskEmit taskEmit1{NES::TaskId(1), NES::PipelineId(1), NES::PipelineId(2), NES::QueryId(1), NES::WorkerThreadId(1), 50, NES::TupleData{}, now + std::chrono::milliseconds(50)};
    replayPrinter->onEvent(taskEmit1);
    
    NES::TaskExecutionComplete taskComplete1{NES::TaskId(1), NES::PipelineId(1), NES::QueryId(1), NES::WorkerThreadId(1), now + std::chrono::milliseconds(60)};
    replayPrinter->onEvent(taskComplete1);
    
    NES::PipelineStop pipelineStop1{NES::PipelineId(1), NES::QueryId(1), NES::WorkerThreadId(1), now + std::chrono::milliseconds(70)};
    replayPrinter->onEvent(pipelineStop1);
    
    NES::QueryStop queryStop1{NES::WorkerThreadId(1), NES::QueryId(1), now + std::chrono::milliseconds(80)};
    replayPrinter->onEvent(queryStop1);
    
    NES::StopQuerySystemEvent stopEvent1{NES::QueryId(1), now + std::chrono::milliseconds(90)};
    replayPrinter->onEvent(stopEvent1);
    
    /// Query 2 events (simultaneous execution)
    NES::SubmitQuerySystemEvent submitEvent2{NES::QueryId(2), "SELECT * FROM test2", now + std::chrono::milliseconds(15)};
    replayPrinter->onEvent(submitEvent2);
    
    NES::StartQuerySystemEvent startEvent2{NES::QueryId(2), now + std::chrono::milliseconds(25)};
    replayPrinter->onEvent(startEvent2);
    
    NES::QueryStart queryStart2{NES::WorkerThreadId(2), NES::QueryId(2), now + std::chrono::milliseconds(35)};
    replayPrinter->onEvent(queryStart2);
    
    NES::PipelineStart pipelineStart2{NES::PipelineId(3), NES::QueryId(2), NES::WorkerThreadId(2), now + std::chrono::milliseconds(45)};
    replayPrinter->onEvent(pipelineStart2);
    
    NES::TaskExecutionStart taskStart2{NES::TaskId(2), NES::PipelineId(3), NES::QueryId(2), NES::WorkerThreadId(2), 200, NES::TupleData{}, now + std::chrono::milliseconds(55)};
    replayPrinter->onEvent(taskStart2);
    
    NES::TaskExecutionComplete taskComplete2{NES::TaskId(2), NES::PipelineId(3), NES::QueryId(2), NES::WorkerThreadId(2), now + std::chrono::milliseconds(65)};
    replayPrinter->onEvent(taskComplete2);
    
    NES::PipelineStop pipelineStop2{NES::PipelineId(3), NES::QueryId(2), NES::WorkerThreadId(2), now + std::chrono::milliseconds(75)};
    replayPrinter->onEvent(pipelineStop2);
    
    NES::QueryStop queryStop2{NES::WorkerThreadId(2), NES::QueryId(2), now + std::chrono::milliseconds(85)};
    replayPrinter->onEvent(queryStop2);
    
    NES::StopQuerySystemEvent stopEvent2{NES::QueryId(2), now + std::chrono::milliseconds(95)};
    replayPrinter->onEvent(stopEvent2);
    
    /// Wait a bit for events to be processed
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    /// Flush and close the printer
    replayPrinter->flush();
    
    std::cout << "Replay data written to test_replay_data directory" << std::endl;
    std::cout << "Each query will have its own file: ReplayData_Query_<ID>_<timestamp>_<pid>.json" << std::endl;
    std::cout << "Note: The replay data includes actual tuple data captured from TupleBuffers." << std::endl;
    std::cout << "Tuple data is serialized as base64-encoded raw bytes for full replay capability." << std::endl;
    return 0;
} 