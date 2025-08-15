#include <gtest/gtest.h>
#include <NESThread.hpp>

namespace NES
{
thread_local WorkerId Thread::WorkerNodeId = WorkerId("Not A Worker");
thread_local std::string Thread::ThreadName = "unnamed";

}
