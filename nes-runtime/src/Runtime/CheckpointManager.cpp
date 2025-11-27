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

#include <Runtime/CheckpointManager.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace NES
{
namespace
{
std::filesystem::path checkpointDirectory{std::filesystem::path{"/tmp"} / "nebulastream"};
std::chrono::milliseconds checkpointInterval{0};
std::unordered_map<std::string, CheckpointManager::Callback> callbacks;
std::mutex checkpointMutex;
std::condition_variable checkpointCv;
std::thread schedulerThread;
std::atomic_bool running = false;

void schedulerLoop()
{
    std::unique_lock lock(checkpointMutex);
    while (running.load())
    {
        if (checkpointInterval.count() == 0)
        {
            checkpointCv.wait(lock, [] { return !running.load(); });
            continue;
        }

        const auto wakeTime = std::chrono::steady_clock::now() + checkpointInterval;
        checkpointCv.wait_until(lock, wakeTime, [] { return !running.load(); });
        if (!running.load())
        {
            break;
        }

        auto callbacksCopy = callbacks;
        lock.unlock();
        for (const auto& [_, callback] : callbacksCopy)
        {
            if (callback)
            {
                callback();
            }
        }
        lock.lock();
    }
}

void ensureScheduler()
{
    if (running.load() || checkpointInterval.count() == 0)
    {
        return;
    }
    running.store(true);
    schedulerThread = std::thread(schedulerLoop);
}

void stopScheduler()
{
    if (!running.load())
    {
        return;
    }
    {
        std::scoped_lock lock(checkpointMutex);
        running.store(false);
    }
    checkpointCv.notify_all();
    if (schedulerThread.joinable())
    {
        schedulerThread.join();
    }
}

}

void CheckpointManager::initialize(std::filesystem::path directory, std::chrono::milliseconds interval)
{
    stopScheduler();
    {
        std::scoped_lock lock(checkpointMutex);
        checkpointDirectory = std::move(directory);
        checkpointInterval = interval;
    }
    ensureScheduler();
}

void CheckpointManager::shutdown()
{
    stopScheduler();
    std::scoped_lock lock(checkpointMutex);
    callbacks.clear();
}

std::filesystem::path CheckpointManager::getCheckpointDirectory()
{
    std::scoped_lock lock(checkpointMutex);
    return checkpointDirectory;
}

std::filesystem::path CheckpointManager::getCheckpointPath(std::string_view fileName)
{
    std::scoped_lock lock(checkpointMutex);
    auto path = checkpointDirectory;
    path /= fileName;
    return path;
}

void CheckpointManager::registerCallback(const std::string& identifier, Callback callback)
{
    std::scoped_lock lock(checkpointMutex);
    callbacks[identifier] = std::move(callback);
}

void CheckpointManager::unregisterCallback(const std::string& identifier)
{
    std::scoped_lock const lock(checkpointMutex);
    callbacks.erase(identifier);
}

}
