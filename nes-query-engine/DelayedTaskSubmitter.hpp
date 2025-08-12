//
// Created by ls on 8/12/25.
//

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <Task.hpp>

namespace NES
{
class DelayedTaskSubmitter
{
public:
    using SubmitFn = std::function<void(Task)>;

private:
    struct ScheduledTask
    {
        Task task;
        std::chrono::steady_clock::time_point deadline;
    };

    // Comparator for priority queue - earlier deadlines have higher priority
    struct TaskComparator
    {
        bool operator()(const ScheduledTask& a, const ScheduledTask& b) const { return a.deadline > b.deadline; }
    };

    SubmitFn submitFn_;
    std::priority_queue<ScheduledTask, std::vector<ScheduledTask>, TaskComparator> taskQueue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::thread workerThread_;
    std::atomic<bool> shutdown_{false};

    void workerLoop()
    {
        std::unique_lock<std::mutex> lock(mutex_);

        while (!shutdown_)
        {
            if (taskQueue_.empty())
            {
                // Wait for tasks to be added or shutdown signal
                cv_.wait(lock, [this] { return !taskQueue_.empty() || shutdown_; });
                continue;
            }

            auto now = std::chrono::steady_clock::now();
            const auto& nextTask = taskQueue_.top();

            if (nextTask.deadline <= now)
            {
                // Task deadline has arrived - execute it
                auto task = std::move(const_cast<ScheduledTask&>(nextTask).task);
                taskQueue_.pop();

                // Release lock while calling submit function to avoid blocking other operations
                lock.unlock();
                submitFn_(std::move(task));
                lock.lock();
            }
            else
            {
                // Wait until the next task deadline or until notified of new task
                cv_.wait_until(lock, nextTask.deadline);
            }
        }
    }

public:
    explicit DelayedTaskSubmitter(SubmitFn submitFn)
        : submitFn_(std::move(submitFn)), workerThread_(&DelayedTaskSubmitter::workerLoop, this)
    {
    }

    ~DelayedTaskSubmitter()
    {
        // Signal shutdown and wake up worker thread
        {
            std::lock_guard<std::mutex> lock(mutex_);
            shutdown_ = true;
        }
        cv_.notify_all();

        // Wait for worker thread to finish
        if (workerThread_.joinable())
        {
            workerThread_.join();
        }
    }

    // Template function to accept any std::chrono::duration type
    template <typename Rep, typename Period>
    void submitTaskIn(Task task, std::chrono::duration<Rep, Period> delay)
    {
        auto deadline = std::chrono::steady_clock::now() + delay;

        std::lock_guard<std::mutex> lock(mutex_);
        bool wasEmpty = taskQueue_.empty();
        bool isEarliest = wasEmpty || deadline < taskQueue_.top().deadline;

        taskQueue_.emplace(ScheduledTask{std::move(task), deadline});

        // Wake up worker thread if this task has an earlier deadline
        if (isEarliest)
        {
            cv_.notify_one();
        }
    }

    // Non-copyable and non-movable for simplicity
    DelayedTaskSubmitter(const DelayedTaskSubmitter&) = delete;
    DelayedTaskSubmitter& operator=(const DelayedTaskSubmitter&) = delete;
    DelayedTaskSubmitter(DelayedTaskSubmitter&&) = delete;
    DelayedTaskSubmitter& operator=(DelayedTaskSubmitter&&) = delete;
};
}
