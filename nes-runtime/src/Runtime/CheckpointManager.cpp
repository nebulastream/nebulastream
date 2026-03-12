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
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
std::filesystem::path checkpointDirectory{std::filesystem::path{"/tmp"} / "nebulastream"};
std::filesystem::path checkpointRecoveryDirectory{};
std::chrono::milliseconds checkpointInterval{0};
bool recoveryEnabled = false;
struct CallbackEntry
{
    explicit CallbackEntry(CheckpointManager::Callback callback) : callback(std::move(callback)) {}

    CheckpointManager::Callback callback;
    std::mutex mutex;
    std::condition_variable cv;
    size_t activeInvocations = 0;
    bool cancelled = false;
};

using CallbackEntryPtr = std::shared_ptr<CallbackEntry>;
using CallbackRegistry = std::map<std::string, CallbackEntryPtr>;
struct CallbackRegistries
{
    CallbackRegistry prepare;
    CallbackRegistry commit;
};

CallbackRegistries callbacks;
std::mutex checkpointMutex;
std::condition_variable checkpointCv;
std::thread schedulerThread;
std::atomic_bool running = false;

std::filesystem::path getAtomicWriteTempPath(const std::filesystem::path& filePath)
{
    return filePath.string() + ".tmp";
}

std::filesystem::path getRecoverySnapshotTempPath(const std::filesystem::path& recoverySnapshotPath)
{
    return recoverySnapshotPath.string() + ".tmp";
}

void replaceFileAtomically(const std::filesystem::path& tempFilePath, const std::filesystem::path& filePath)
{
    std::error_code ec;
    std::filesystem::rename(tempFilePath, filePath, ec);
    if (ec)
    {
        std::filesystem::remove(tempFilePath, ec);
        throw CheckpointError("Cannot replace checkpoint file {}; {}", filePath.string(), ec.message());
    }
}

void appendEntryIfPresent(std::vector<CallbackEntryPtr>& entries, CallbackRegistry& registry, const std::string& identifier)
{
    if (const auto it = registry.find(identifier); it != registry.end())
    {
        entries.emplace_back(it->second);
        registry.erase(it);
    }
}

void cancelAndWaitForEntry(const CallbackEntryPtr& entry)
{
    if (!entry)
    {
        return;
    }

    std::unique_lock lock(entry->mutex);
    entry->cancelled = true;
    entry->cv.wait(lock, [&entry] { return entry->activeInvocations == 0; });
}

class CallbackInvocationGuard
{
public:
    explicit CallbackInvocationGuard(CallbackEntryPtr entry) : entry(std::move(entry)) {}

    ~CallbackInvocationGuard()
    {
        if (!entry)
        {
            return;
        }

        std::scoped_lock lock(entry->mutex);
        INVARIANT(entry->activeInvocations > 0, "Callback invocation guard encountered a negative active invocation count");
        entry->activeInvocations -= 1;
        if (entry->activeInvocations == 0)
        {
            entry->cv.notify_all();
        }
    }

private:
    CallbackEntryPtr entry;
};

bool tryAcquireInvocation(const CallbackEntryPtr& entry)
{
    std::scoped_lock lock(entry->mutex);
    if (entry->cancelled)
    {
        return false;
    }
    entry->activeInvocations += 1;
    return true;
}

void runCallbacksInOrder(const std::vector<CallbackEntryPtr>& phaseCallbacks)
{
    for (const auto& entry : phaseCallbacks)
    {
        if (!entry || !tryAcquireInvocation(entry))
        {
            continue;
        }

        CallbackInvocationGuard guard(entry);
        if (entry->callback)
        {
            entry->callback();
        }
    }
}

void executeCheckpointRound()
{
    std::vector<CallbackEntryPtr> prepareCallbacks;
    std::vector<CallbackEntryPtr> commitCallbacks;
    {
        std::scoped_lock lock(checkpointMutex);
        prepareCallbacks.reserve(callbacks.prepare.size());
        commitCallbacks.reserve(callbacks.commit.size());
        for (const auto& [_, entry] : callbacks.prepare)
        {
            prepareCallbacks.emplace_back(entry);
        }
        for (const auto& [_, entry] : callbacks.commit)
        {
            commitCallbacks.emplace_back(entry);
        }
    }

    runCallbacksInOrder(prepareCallbacks);
    runCallbacksInOrder(commitCallbacks);
}

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

        lock.unlock();
        executeCheckpointRound();
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

void CheckpointManager::initialize(std::filesystem::path directory, std::chrono::milliseconds interval, bool recoverFromCheckpoint)
{
    stopScheduler();
    std::filesystem::path recoverySnapshotPath;
    {
        std::scoped_lock lock(checkpointMutex);
        checkpointDirectory = std::move(directory);
        checkpointRecoveryDirectory = checkpointDirectory / ".recovery_snapshot";
        checkpointInterval = interval;
        recoveryEnabled = recoverFromCheckpoint;
        recoverySnapshotPath = checkpointRecoveryDirectory;
    }

    std::error_code ec;
    const auto recoverySnapshotTempPath = getRecoverySnapshotTempPath(recoverySnapshotPath);
    std::filesystem::remove_all(recoverySnapshotPath, ec);
    if (!ec)
    {
        std::filesystem::remove_all(recoverySnapshotTempPath, ec);
    }
    if (recoverFromCheckpoint && std::filesystem::exists(checkpointDirectory, ec) && std::filesystem::is_directory(checkpointDirectory, ec))
    {
        std::filesystem::create_directories(recoverySnapshotTempPath, ec);
        for (std::filesystem::directory_iterator it(checkpointDirectory, ec); !ec && it != std::filesystem::directory_iterator(); ++it)
        {
            const auto& entry = *it;
            if (entry.path().filename() == recoverySnapshotPath.filename() || entry.path().filename() == recoverySnapshotTempPath.filename())
            {
                continue;
            }
            const auto destination = recoverySnapshotTempPath / entry.path().filename();
            std::filesystem::copy(
                entry.path(),
                destination,
                std::filesystem::copy_options::recursive | std::filesystem::copy_options::overwrite_existing,
                ec);
            if (ec)
            {
                break;
            }
        }

        if (!ec)
        {
            std::filesystem::rename(recoverySnapshotTempPath, recoverySnapshotPath, ec);
        }
    }

    if (ec)
    {
        std::error_code cleanupEc;
        std::filesystem::remove_all(recoverySnapshotTempPath, cleanupEc);
    }

    if (ec)
    {
        NES_WARNING("Failed to prepare recovery checkpoint snapshot in {}: {}", recoverySnapshotPath.string(), ec.message());
    }
    ensureScheduler();
}

void CheckpointManager::shutdown()
{
    stopScheduler();
    std::vector<CallbackEntryPtr> entriesToCancel;
    {
        std::scoped_lock lock(checkpointMutex);
        entriesToCancel.reserve(callbacks.prepare.size() + callbacks.commit.size());
        for (const auto& [_, entry] : callbacks.prepare)
        {
            entriesToCancel.emplace_back(entry);
        }
        for (const auto& [_, entry] : callbacks.commit)
        {
            entriesToCancel.emplace_back(entry);
        }
        callbacks.prepare.clear();
        callbacks.commit.clear();
    }
    for (const auto& entry : entriesToCancel)
    {
        cancelAndWaitForEntry(entry);
    }
}

std::filesystem::path CheckpointManager::getCheckpointDirectory()
{
    std::scoped_lock lock(checkpointMutex);
    return checkpointDirectory;
}

std::filesystem::path CheckpointManager::getCheckpointRecoveryDirectory()
{
    std::scoped_lock lock(checkpointMutex);
    if (!checkpointRecoveryDirectory.empty() && std::filesystem::exists(checkpointRecoveryDirectory))
    {
        return checkpointRecoveryDirectory;
    }
    return checkpointDirectory;
}

std::filesystem::path CheckpointManager::getCheckpointPath(std::string_view fileName)
{
    std::scoped_lock lock(checkpointMutex);
    auto path = checkpointDirectory;
    path /= fileName;
    return path;
}

void CheckpointManager::persistFile(std::string_view fileName, std::string_view contents)
{
    const auto filePath = getCheckpointPath(fileName);
    const auto tempFilePath = getAtomicWriteTempPath(filePath);
    const auto directory = filePath.parent_path();
    if (!directory.empty())
    {
        std::filesystem::create_directories(directory);
    }

    std::ofstream out(tempFilePath, std::ios::binary | std::ios::trunc);
    if (!out.is_open())
    {
        throw CheckpointError("Cannot open checkpoint file {}", tempFilePath.string());
    }
    out.write(contents.data(), static_cast<std::streamsize>(contents.size()));
    out.flush();
    if (!out)
    {
        throw CheckpointError("Cannot write checkpoint file {}", tempFilePath.string());
    }
    out.close();
    if (!out)
    {
        throw CheckpointError("Cannot close checkpoint file {}", tempFilePath.string());
    }
    replaceFileAtomically(tempFilePath, filePath);
}

std::optional<std::string> CheckpointManager::loadFile(const std::filesystem::path& absolutePath)
{
    std::ifstream in(absolutePath, std::ios::binary);
    if (!in.is_open())
    {
        return std::nullopt;
    }
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

bool CheckpointManager::isCheckpointingEnabled()
{
    std::scoped_lock lock(checkpointMutex);
    return checkpointInterval.count() > 0;
}

bool CheckpointManager::shouldRecoverFromCheckpoint()
{
    std::scoped_lock lock(checkpointMutex);
    return recoveryEnabled;
}

void CheckpointManager::registerCallback(const std::string& identifier, Callback callback, const CallbackPhase phase)
{
    std::vector<CallbackEntryPtr> replacedEntries;
    {
        std::scoped_lock lock(checkpointMutex);
        auto newEntry = std::make_shared<CallbackEntry>(std::move(callback));
        switch (phase)
        {
            case CallbackPhase::Prepare: {
                appendEntryIfPresent(replacedEntries, callbacks.prepare, identifier);
                appendEntryIfPresent(replacedEntries, callbacks.commit, identifier);
                callbacks.prepare.emplace(identifier, std::move(newEntry));
                break;
            }
            case CallbackPhase::Commit: {
                appendEntryIfPresent(replacedEntries, callbacks.prepare, identifier);
                appendEntryIfPresent(replacedEntries, callbacks.commit, identifier);
                callbacks.commit.emplace(identifier, std::move(newEntry));
                break;
            }
        }
    }

    for (const auto& entry : replacedEntries)
    {
        cancelAndWaitForEntry(entry);
    }
}

void CheckpointManager::unregisterCallback(const std::string& identifier)
{
    std::vector<CallbackEntryPtr> removedEntries;
    {
        std::scoped_lock const lock(checkpointMutex);
        appendEntryIfPresent(removedEntries, callbacks.prepare, identifier);
        appendEntryIfPresent(removedEntries, callbacks.commit, identifier);
    }

    for (const auto& entry : removedEntries)
    {
        cancelAndWaitForEntry(entry);
    }
}

void CheckpointManager::runCallbacksOnce()
{
    executeCheckpointRound();
}

}
