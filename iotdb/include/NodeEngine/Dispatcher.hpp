#ifndef INCLUDE_DISPATCHER_H_
#define INCLUDE_DISPATCHER_H_

#include <condition_variable>
#include <map>
#include <mutex>
#include <thread>
#include <vector>
#include <chrono>

#include <CodeGen/QueryExecutionPlan.hpp>
#include <Core/TupleBuffer.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <Runtime/DataSource.hpp>
#include <NodeEngine/Task.hpp>


namespace iotdb {
using NanoSeconds = std::chrono::nanoseconds;
using Clock = std::chrono::high_resolution_clock;

//Timestamp getTimestamp() { return std::chrono::duration_cast<NanoSeconds>(Clock::now().time_since_epoch()).count(); }


class Dispatcher {
  public:
    void registerQuery(const QueryExecutionPlanPtr);
    void deregisterQuery(const QueryExecutionPlanPtr);

    TaskPtr getWork(bool& run_thread);
    void addWork(const TupleBufferPtr, DataSource*);
    void completedWork(TaskPtr task);

    void printStatistics(const QueryExecutionPlanPtr qep);

    static Dispatcher& instance();

    void unblockThreads() { cv.notify_all(); }
    const size_t getProcessedTasks() const { return processedTasks; }
    void resetDispatcher();

  private:
    /* implement singleton semantics: no construction,
     * copying or destruction of Dispatcher objects
     * outside of the class */
    Dispatcher();
    Dispatcher(const Dispatcher&);
    Dispatcher& operator=(const Dispatcher&);
    ~Dispatcher();

    std::vector<TaskPtr> task_queue;
    std::map<DataSource*, std::vector<QueryExecutionPlanPtr>> source_to_query_map;
    std::map<Window*, std::vector<QueryExecutionPlanPtr>> window_to_query_map;
    std::map<DataSink*, std::vector<QueryExecutionPlanPtr>> sink_to_query_map;

    std::mutex bufferMutex;
    std::mutex queryMutex;
    std::mutex workMutex;

    std::condition_variable cv;

    // statistics:
    std::atomic<size_t> workerHitEmptyTaskQueue;
    std::atomic<size_t> processedTasks;
    std::atomic<size_t> processedTuple;

    std::atomic<size_t> processedBuffers;
    size_t startTime;
};
typedef std::shared_ptr<Dispatcher> DispatcherPtr;
} // namespace iotdb

#endif /* INCLUDE_DISPATCHER_H_ */
