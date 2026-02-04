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

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include <SystestState.hpp>

namespace NES::Systest
{
class InlineEventController
{
public:
    using LifecycleCallback = std::function<void()>;

    explicit InlineEventController(InlineEventScript script);
    ~InlineEventController();

    InlineEventController(const InlineEventController&) = delete;
    InlineEventController& operator=(const InlineEventController&) = delete;

    [[nodiscard]] uint16_t getPort() const { return port; }

    /// Set callbacks that are invoked when scripted actions request worker crash or restart.
    void setLifecycleCallbacks(LifecycleCallback crashCb, LifecycleCallback restartCb);
    /// Stop the server and unblock waiters.
    void stop();
    /// Restart the server on the same port and reset the script to the beginning.
    void restart(bool resetScriptProgress = false);

private:
    [[nodiscard]] bool requiresConnection(const InlineAction& action) const;
    void closeListenSocket();
    void runServer();
    int acceptClient();

    InlineEventScript script;
    std::jthread serverThread;
    uint16_t port = 0;
    std::atomic<int> listenFd{-1};
    std::atomic_bool stopped{false};
    std::atomic_bool serverReady{false};
    std::atomic_bool serverError{false};
    std::atomic<size_t> nextActionIndex{0};
    std::condition_variable readyCv;
    std::mutex readyMutex;
    std::mutex lifecycleMutex;
    LifecycleCallback crashCallback;
    LifecycleCallback restartCallback;
};

}
