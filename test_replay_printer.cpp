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

    // Query 1 events
    NES::QueryStart queryStart1{NES::WorkerThreadId(1), NES::QueryId(1), now};
    replayPrinter->onEvent(queryStart1);

    NES::PipelineStart pipelineStart1{NES::WorkerThreadId(1), NES::QueryId(1), NES::PipelineId(1), now + std::chrono::milliseconds(30)};
    replayPrinter->onEvent(pipelineStart1);
    
    NES::TaskExecutionStart taskStart1{NES::WorkerThreadId(1), NES::QueryId(1), NES::PipelineId(1), NES::TaskId(1), 100, NES::TupleData{}, now + std::chrono::milliseconds(40)};
    replayPrinter->onEvent(taskStart1);
    
    NES::TaskEmit taskEmit1{NES::WorkerThreadId(1), NES::QueryId(1), NES::PipelineId(1), NES::PipelineId(2), NES::TaskId(1), 50, NES::TupleData{}, now + std::chrono::milliseconds(50)};
    replayPrinter->onEvent(taskEmit1);

    // Simulate storing compiled libraries for Query 1
    replayPrinter->storeCompiledLibrary(NES::QueryId(1), "/tmp/dump/query1/libpipeline_1.so");
    replayPrinter->storeCompiledLibrary(NES::QueryId(1), "/tmp/dump/query1/libpipeline_2.so");

    // Query 2 events
    NES::QueryStart queryStart2{NES::WorkerThreadId(2), NES::QueryId(2), now + std::chrono::milliseconds(100)};
    replayPrinter->onEvent(queryStart2);

    NES::PipelineStart pipelineStart2{NES::WorkerThreadId(2), NES::QueryId(2), NES::PipelineId(3), now + std::chrono::milliseconds(130)};
    replayPrinter->onEvent(pipelineStart2);
    
    NES::TaskExecutionStart taskStart2{NES::WorkerThreadId(2), NES::QueryId(2), NES::PipelineId(3), NES::TaskId(2), 200, NES::TupleData{}, now + std::chrono::milliseconds(140)};
    replayPrinter->onEvent(taskStart2);

    // Simulate storing compiled libraries for Query 2
    replayPrinter->storeCompiledLibrary(NES::QueryId(2), "/tmp/dump/query2/libpipeline_3.so");

    // Complete the queries
    NES::PipelineStop pipelineStop1{NES::WorkerThreadId(1), NES::QueryId(1), NES::PipelineId(1), now + std::chrono::milliseconds(200)};
    replayPrinter->onEvent(pipelineStop1);
    
    NES::QueryStop queryStop1{NES::WorkerThreadId(1), NES::QueryId(1), now + std::chrono::milliseconds(250)};
    replayPrinter->onEvent(queryStop1);

    NES::PipelineStop pipelineStop2{NES::WorkerThreadId(2), NES::QueryId(2), NES::PipelineId(3), now + std::chrono::milliseconds(300)};
    replayPrinter->onEvent(pipelineStop2);
    
    NES::QueryStop queryStop2{NES::WorkerThreadId(2), NES::QueryId(2), now + std::chrono::milliseconds(350)};
    replayPrinter->onEvent(queryStop2);

    /// Flush to ensure all data is written
    replayPrinter->flush();

    std::cout << "Replay data written to test_replay_data directory" << std::endl;
    std::cout << "Each query will have its own file: ReplayData_Query_<ID>_<timestamp>_<pid>.json" << std::endl;
    std::cout << "Compiled libraries and artifacts are stored in: test_replay_data/libraries/" << std::endl;
    std::cout << "Note: The replay data includes actual tuple data captured from TupleBuffers." << std::endl;
    std::cout << "Tuple data is serialized as base64-encoded raw bytes for full replay capability." << std::endl;
    std::cout << "Compiled libraries and MLIR artifacts are captured and stored with their file paths for complete replay." << std::endl;
    return 0;
} 