#include <gtest/gtest.h>
#include <NESThread.hpp>

namespace NES
{
TEST(ThreadTest, BasicNaming)
{
    Thread t(
        "TestThread",
        WorkerId(1),
        []()
        {
            EXPECT_EQ(Thread::getThisThreadName(), "TestThread");
            EXPECT_EQ(WorkerId(1), Thread::getThisWorkerNodeId());
            Thread t1(
                "Test-Inner-Thread",
                []()
                {
                    EXPECT_EQ(Thread::getThisThreadName(), "Test-Inner-Thread");
                    EXPECT_EQ(WorkerId(1), Thread::getThisWorkerNodeId());
                });

            Thread t2(
                "Test-Inner-Thread2",
                WorkerId(2),
                []()
                {
                    EXPECT_EQ(Thread::getThisThreadName(), "Test-Inner-Thread2");
                    EXPECT_EQ(WorkerId(2), Thread::getThisWorkerNodeId());
                });
        });
}
}
