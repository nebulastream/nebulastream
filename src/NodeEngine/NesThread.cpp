#include <NodeEngine/NesThread.hpp>


namespace NES {

/// The first thread will have index 0.
std::atomic<uint32_t> NesThread::next_index{ 0 };

/// No thread IDs have been used yet.
std::atomic<bool> NesThread::id_used[MaxNumThreads] = {};

/// Give the new thread an ID.
thread_local NesThread::ThreadId NesThread::id{};

}