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

#include <InlineEventServer.hpp>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <deque>
#include <functional>
#include <string>
#include <thread>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <ErrorHandling.hpp>

namespace NES::Systest
{
namespace
{
constexpr int Backlog = 1;
constexpr std::chrono::milliseconds SocketRetry{50};
}

bool InlineEventController::requiresConnection(const InlineAction& action) const
{
    return action.type == InlineActionType::SendTuple;
}

void InlineEventController::closeListenSocket()
{
    const int fd = listenFd.exchange(-1);
    if (fd >= 0)
    {
        ::shutdown(fd, SHUT_RDWR);
        ::close(fd);
    }
}

InlineEventController::InlineEventController(InlineEventScript script) : script(std::move(script))
{
    listenFd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd.load() < 0)
    {
        throw TestException("Failed to create socket for inline event server: {}", strerror(errno));
    }

    const int reuseAddr = 1;
    ::setsockopt(listenFd.load(), SOL_SOCKET, SO_REUSEADDR, &reuseAddr, sizeof(reuseAddr));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0; /// let the OS pick

    if (bind(listenFd.load(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
    {
        closeListenSocket();
        throw TestException("Failed to bind inline event server socket: {}", strerror(errno));
    }

    socklen_t addrLen = sizeof(addr);
    if (getsockname(listenFd.load(), reinterpret_cast<sockaddr*>(&addr), &addrLen) < 0)
    {
        closeListenSocket();
        throw TestException("Failed to get socket name for inline event server: {}", strerror(errno));
    }
    port = ntohs(addr.sin_port);

    serverThread = std::jthread([this](const std::stop_token&) { runServer(); });
    {
        std::unique_lock lock(readyMutex);
        readyCv.wait(lock, [this] { return serverReady.load() || serverError.load(); });
    }
    if (serverError.load())
    {
        throw TestException("Failed to start inline event server listener");
    }
}

InlineEventController::~InlineEventController()
{
    stop();
}

void InlineEventController::setLifecycleCallbacks(LifecycleCallback crashCb, LifecycleCallback restartCb)
{
    std::scoped_lock lock(lifecycleMutex);
    crashCallback = std::move(crashCb);
    restartCallback = std::move(restartCb);
}

void InlineEventController::stop()
{
    stopped.store(true);
    closeListenSocket();
    {
        std::unique_lock lock(readyMutex);
        serverReady.store(false);
        serverError.store(false);
    }
    readyCv.notify_all();
    if (serverThread.joinable())
    {
        serverThread.request_stop();
        serverThread.join();
    }
}

void InlineEventController::restart(const bool resetScriptProgress)
{
    stop();
    stopped.store(false);
    serverError.store(false);
    if (resetScriptProgress)
    {
        nextActionIndex.store(0);
    }

    listenFd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd.load() < 0)
    {
        throw TestException("Failed to create socket for inline event server: {}", strerror(errno));
    }

    const int reuseAddr = 1;
    ::setsockopt(listenFd.load(), SOL_SOCKET, SO_REUSEADDR, &reuseAddr, sizeof(reuseAddr));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);

    if (bind(listenFd.load(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
    {
        closeListenSocket();
        throw TestException("Failed to bind inline event server socket: {}", strerror(errno));
    }

    serverThread = std::jthread([this](const std::stop_token&) { runServer(); });
    {
        std::unique_lock lock(readyMutex);
        readyCv.wait(lock, [this] { return serverReady.load() || serverError.load(); });
    }
    if (serverError.load())
    {
        throw TestException("Failed to restart inline event server listener");
    }
}

int InlineEventController::acceptClient()
{
    while (!stopped.load())
    {
        int clientFd = ::accept(listenFd.load(), nullptr, nullptr);
        if (clientFd >= 0)
        {
            return clientFd;
        }
        if (stopped.load())
        {
            break;
        }
        std::this_thread::sleep_for(SocketRetry);
    }
    return -1;
}

void InlineEventController::runServer()
{
    const int fd = listenFd.load();
    if (listen(fd, Backlog) < 0)
    {
        {
            std::scoped_lock lock(readyMutex);
            serverError.store(true);
        }
        readyCv.notify_all();
        closeListenSocket();
        return;
    }
    {
        std::scoped_lock lock(readyMutex);
        serverReady.store(true);
        serverError.store(false);
    }
    readyCv.notify_all();

    size_t actionIdx = nextActionIndex.load();
    int clientFd = -1;
    std::deque<InlineAction> deferredSendActions;
    bool workerReady = true;

    while (!stopped.load() && (actionIdx < script.actions.size() || !deferredSendActions.empty()))
    {
        const bool useDeferredAction = workerReady && !deferredSendActions.empty();
        const InlineAction* actionPtr = nullptr;
        if (useDeferredAction)
        {
            actionPtr = &deferredSendActions.front();
        }
        else if (actionIdx < script.actions.size())
        {
            actionPtr = &script.actions[actionIdx];
            nextActionIndex.store(actionIdx);
        }

        if (actionPtr == nullptr)
        {
            break;
        }

        const InlineAction& action = *actionPtr;

        if (action.type == InlineActionType::SendTuple && !workerReady)
        {
            /// Drop tuples while the worker is down.
            ++actionIdx;
            nextActionIndex.store(actionIdx);
            continue;
        }

        if (requiresConnection(action) && clientFd < 0)
        {
            clientFd = acceptClient();
            if (clientFd < 0)
            {
                break;
            }
        }

        auto popAction = [&](bool consumedFromDeferred)
        {
            if (consumedFromDeferred)
            {
                deferredSendActions.pop_front();
            }
            else
            {
                ++actionIdx;
            }
            nextActionIndex.store(actionIdx);
        };

        switch (action.type)
        {
            case InlineActionType::SendTuple: {
                const std::string line = action.tuple + "\n";
                if (::send(clientFd, line.data(), line.size(), 0) < 0)
                {
                    ::shutdown(clientFd, SHUT_RDWR);
                    ::close(clientFd);
                    clientFd = -1;
                    /// Keep the action deferred to retry after reconnection.
                    if (!useDeferredAction)
                    {
                        deferredSendActions.push_front(action);
                    }
                }
                else
                {
                    popAction(useDeferredAction);
                }
                break;
            }
            case InlineActionType::DelayMs:
                if (action.payload)
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(*action.payload));
                }
                popAction(useDeferredAction);
                break;
            case InlineActionType::CrashWorker: {
                LifecycleCallback cb;
                {
                    std::scoped_lock lock(lifecycleMutex);
                    cb = crashCallback;
                }
                if (cb)
                {
                    cb();
                }
                workerReady = false;
                deferredSendActions.clear();
                if (clientFd >= 0)
                {
                    ::shutdown(clientFd, SHUT_RDWR);
                    ::close(clientFd);
                    clientFd = -1;
                }
                popAction(useDeferredAction);
                break;
            }
            case InlineActionType::RestartWorker: {
                LifecycleCallback cb;
                {
                    std::scoped_lock lock(lifecycleMutex);
                    cb = restartCallback;
                }
                if (cb)
                {
                    cb();
                }
                workerReady = true;
                if (clientFd >= 0)
                {
                    ::shutdown(clientFd, SHUT_RDWR);
                    ::close(clientFd);
                    clientFd = -1;
                }
                popAction(useDeferredAction);
                break;
            }
        }
    }

    if (clientFd >= 0)
    {
        ::shutdown(clientFd, SHUT_RDWR);
        ::close(clientFd);
    }
    stopped.store(true);
    {
        std::scoped_lock lock(readyMutex);
        serverReady.store(false);
    }
    readyCv.notify_all();
    closeListenSocket();
}

}
