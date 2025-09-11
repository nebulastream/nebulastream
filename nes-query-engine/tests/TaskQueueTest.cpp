//
// Created by ls on 9/10/25.
//

#include <gtest/gtest.h>


#include <TaskQueue.h>

namespace NES
{
TEST(ThisIsATest, ThisIsATest2)
{
    TaskQueue<int> queue;

    std::jthread source(
        [&queue](std::stop_token token)
        {
            while (!token.stop_requested())
            {
                queue.admissionTask(token, 324);
            }
        });

    std::jthread source2(
        [&queue](std::stop_token token)
        {
            while (!token.stop_requested())
            {
                queue.admissionTask(token, 323);
            }
        });

    std::jthread worker(
        [&queue](std::stop_token token)
        {
            while (!token.stop_requested())
            {
                if (auto task = queue.nextTask(token))
                {
                    if (*task == 324)
                    {
                        queue.internalTask(323);
                    }
                }
            }
        });
    std::jthread worker1(
        [&queue](std::stop_token token)
        {
            while (!token.stop_requested())
            {
                if (auto task = queue.nextTask(token))
                {
                    if (*task == 324)
                    {
                        queue.internalTask(323);
                    }
                }
            }
        });
    std::jthread worker2(
        [&queue](std::stop_token token)
        {
            while (!token.stop_requested())
            {
                if (auto task = queue.nextTask(token))
                {
                    if (*task == 324)
                    {
                        queue.internalTask(323);
                    }
                }
            }
        });
    std::jthread worker3(
        [&queue](std::stop_token token)
        {
            while (!token.stop_requested())
            {
                if (auto task = queue.nextTask(token))
                {
                    if (*task == 324)
                    {
                        queue.internalTask(323);
                    }
                }
            }
        });

    std::this_thread::sleep_for(std::chrono::seconds(10));
}
}