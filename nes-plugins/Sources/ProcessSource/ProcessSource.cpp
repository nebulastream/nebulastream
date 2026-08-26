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

#include <ProcessSource.hpp>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <format>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/RawSource.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <sys/poll.h>
#include <sys/wait.h>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{
namespace
{
constexpr auto STOP_POLL_INTERVAL = std::chrono::milliseconds{25};
constexpr auto TERMINATION_GRACE_PERIOD = std::chrono::milliseconds{250};
constexpr auto BASH_COMMAND_WRAPPER = "PID=$1; pid=$1; WORKER_PID=$1; SOURCE_ID=$2; INVOCATION=$3; TIMESTAMP_MS=$4; "
                                      "export PID pid WORKER_PID SOURCE_ID INVOCATION TIMESTAMP_MS; "
                                      "__NES_PROCESS_SOURCE_COMMAND=$5; shift 5; eval \"$__NES_PROCESS_SOURCE_COMMAND\"";

int timeoutUntil(const std::chrono::steady_clock::time_point timePoint)
{
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(timePoint - std::chrono::steady_clock::now());
    return static_cast<int>(std::clamp<int64_t>(remaining.count(), 0, STOP_POLL_INTERVAL.count()));
}
}

ProcessSource::ProcessSource(const SourceDescriptor& sourceDescriptor)
    : RawSource(RawSource::requiredTailPaddingFor(sourceDescriptor.getInputFormatterDescriptor().getInputFormatterType()))
    , command(sourceDescriptor.getFromConfig(ConfigParametersProcessSource::COMMAND))
    , sourceId(sourceDescriptor.getPhysicalSourceId().getRawValue())
    , refreshInterval(
          std::chrono::milliseconds{
              static_cast<int64_t>(sourceDescriptor.getFromConfig(ConfigParametersProcessSource::REFRESH_INTERVAL_MS))})
    , flushInterval(
          std::chrono::milliseconds{static_cast<int64_t>(sourceDescriptor.getFromConfig(ConfigParametersProcessSource::FLUSH_INTERVAL_MS))})
{
}

ProcessSource::~ProcessSource()
{
    terminateProcess();
}

void ProcessSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    PRECONDITION(!isOpen, "ProcessSource was opened multiple times");
    isOpen = true;
    nextInvocation = 1;
    nextRefresh = std::chrono::steady_clock::now();
    NES_DEBUG("Opening ProcessSource with refresh interval {} and flush interval {}", refreshInterval, flushInterval);
}

void ProcessSource::close()
{
    PRECONDITION(isOpen, "ProcessSource was closed multiple times or never opened");
    terminateProcess();
    isOpen = false;
    NES_DEBUG("Closed ProcessSource");
}

void ProcessSource::startProcess()
{
    PRECONDITION(processFinished(), "Cannot start overlapping ProcessSource command invocations");

    const auto workerPid = std::to_string(getpid());
    const auto sourceIdString = std::to_string(sourceId);
    const auto invocation = std::to_string(nextInvocation);
    const auto timestamp = std::to_string(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());

    std::array<int, 2> pipeFds{};
    if (pipe2(pipeFds.data(), O_CLOEXEC) == -1)
    {
        throw CannotOpenSource("Could not create ProcessSource output pipe: {}", std::strerror(errno));
    }

    const pid_t pid = fork();
    if (pid == -1)
    {
        const auto savedErrno = errno;
        ::close(pipeFds[0]);
        ::close(pipeFds[1]);
        throw CannotOpenSource("Could not fork ProcessSource command: {}", std::strerror(savedErrno));
    }

    if (pid == 0)
    {
        /// Everything between fork and exec must be async-signal-safe. A separate process group lets
        /// close() terminate the complete Bash pipeline rather than only the shell process.
        setpgid(0, 0);
        ::close(pipeFds[0]);
        if (dup2(pipeFds[1], STDOUT_FILENO) == -1)
        {
            _exit(126);
        }
        ::close(pipeFds[1]);
        execl( /// NOLINT(cppcoreguidelines-pro-type-vararg) - POSIX execl is async-signal-safe after fork
            "/bin/bash",
            "bash",
            "-c",
            BASH_COMMAND_WRAPPER,
            "nes-process-source",
            workerPid.c_str(),
            sourceIdString.c_str(),
            invocation.c_str(),
            timestamp.c_str(),
            command.c_str(),
            static_cast<char*>(nullptr));
        _exit(127);
    }

    ::close(pipeFds[1]);
    const auto flags = fcntl(pipeFds[0], F_GETFL, 0); /// NOLINT(cppcoreguidelines-pro-type-vararg) - POSIX API
    if (flags == -1 || fcntl(pipeFds[0], F_SETFL, flags | O_NONBLOCK) == -1) /// NOLINT(cppcoreguidelines-pro-type-vararg) - POSIX API
    {
        const auto savedErrno = errno;
        ::close(pipeFds[0]);
        kill(-pid, SIGKILL);
        kill(pid, SIGKILL);
        waitpid(pid, nullptr, 0);
        throw CannotOpenSource("Could not configure ProcessSource output pipe: {}", std::strerror(savedErrno));
    }

    /// The child also calls setpgid before exec. Calling it here closes the race with an immediate stop request.
    setpgid(pid, pid);
    childPid = pid;
    processGroupId = pid;
    standardOutputFd = pipeFds[0];
    nextRefresh = std::chrono::steady_clock::now() + refreshInterval;
    NES_TRACE("Started ProcessSource command invocation {} with pid {}", nextInvocation, pid);
    ++nextInvocation;
}

void ProcessSource::reapProcess()
{
    if (childPid > 0)
    {
        int status = 0;
        const auto result = waitpid(childPid, &status, WNOHANG);
        if (result == childPid)
        {
            if (WIFEXITED(status) && WEXITSTATUS(status) != 0)
            {
                NES_WARNING("ProcessSource command exited with status {}", WEXITSTATUS(status));
            }
            else if (WIFSIGNALED(status))
            {
                NES_WARNING("ProcessSource command was terminated by signal {}", WTERMSIG(status));
            }
            childPid = -1;
        }
        else if (result == -1 && errno == ECHILD)
        {
            childPid = -1;
        }
    }

    if (processFinished())
    {
        processGroupId = -1;
    }
}

void ProcessSource::terminateProcess() noexcept
{
    if (standardOutputFd >= 0)
    {
        ::close(standardOutputFd);
        standardOutputFd = -1;
    }

    if (processGroupId > 0)
    {
        if (kill(-processGroupId, SIGTERM) == -1 && errno == ESRCH && childPid > 0)
        {
            kill(childPid, SIGTERM);
        }
    }

    const auto deadline = std::chrono::steady_clock::now() + TERMINATION_GRACE_PERIOD;
    while (childPid > 0 && std::chrono::steady_clock::now() < deadline)
    {
        const auto result = waitpid(childPid, nullptr, WNOHANG);
        if (result == childPid || (result == -1 && errno == ECHILD))
        {
            childPid = -1;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }

    if (processGroupId > 0)
    {
        kill(-processGroupId, SIGKILL);
    }
    if (childPid > 0)
    {
        kill(childPid, SIGKILL);
        while (waitpid(childPid, nullptr, 0) == -1 && errno == EINTR)
        {
        }
        childPid = -1;
    }
    processGroupId = -1;
}

size_t ProcessSource::readAvailable(std::span<std::byte> out)
{
    if (standardOutputFd < 0 || out.empty())
    {
        return 0;
    }

    const auto bytesRead = ::read(standardOutputFd, out.data(), out.size());
    if (bytesRead > 0)
    {
        return static_cast<size_t>(bytesRead);
    }
    if (bytesRead == 0)
    {
        ::close(standardOutputFd);
        standardOutputFd = -1;
        reapProcess();
        return 0;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
    {
        return 0;
    }
    throw CannotOpenSource("Could not read ProcessSource command output: {}", std::strerror(errno));
}

bool ProcessSource::processFinished() const noexcept
{
    return childPid < 0 && standardOutputFd < 0;
}

Source::FillTupleBufferResult ProcessSource::fillRaw(std::span<std::byte> out, const std::stop_token& stopToken)
{
    PRECONDITION(isOpen, "Cannot fill a closed ProcessSource");
    size_t writtenBytes = 0;
    std::optional<std::chrono::steady_clock::time_point> flushDeadline;

    while (!stopToken.stop_requested())
    {
        reapProcess();
        const auto now = std::chrono::steady_clock::now();
        if (processFinished() && now >= nextRefresh)
        {
            startProcess();
        }

        if (flushDeadline && now >= *flushDeadline)
        {
            return FillTupleBufferResult::withBytes(writtenBytes);
        }

        int timeout = STOP_POLL_INTERVAL.count();
        if (flushDeadline)
        {
            timeout = timeoutUntil(*flushDeadline);
        }
        else if (processFinished())
        {
            timeout = timeoutUntil(nextRefresh);
        }

        if (standardOutputFd < 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds{timeout});
            continue;
        }

        pollfd outputPoll{.fd = standardOutputFd, .events = POLLIN, .revents = 0};
        const auto pollResult = poll(&outputPoll, 1, timeout);
        if (pollResult == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }
            throw CannotOpenSource("Could not poll ProcessSource command output: {}", std::strerror(errno));
        }
        if (pollResult == 0)
        {
            continue;
        }

        if ((outputPoll.revents & (POLLIN | POLLHUP)) != 0)
        {
            const auto bytesRead = readAvailable(out.subspan(writtenBytes));
            if (bytesRead > 0)
            {
                if (writtenBytes == 0)
                {
                    flushDeadline = std::chrono::steady_clock::now() + flushInterval;
                }
                writtenBytes += bytesRead;
                if (writtenBytes == out.size())
                {
                    return FillTupleBufferResult::withBytes(writtenBytes);
                }
            }
        }
        if ((outputPoll.revents & (POLLERR | POLLNVAL)) != 0)
        {
            throw CannotOpenSource("ProcessSource command output pipe failed");
        }
    }

    terminateProcess();
    if (writtenBytes > 0)
    {
        return FillTupleBufferResult::withBytes(writtenBytes);
    }
    return FillTupleBufferResult::eos();
}

std::ostream& ProcessSource::toString(std::ostream& str) const
{
    return str << std::format(
               "ProcessSource(command='{}', refresh_interval={}, flush_interval={})", command, refreshInterval, flushInterval);
}

DescriptorConfig::Config ProcessSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersProcessSource>(std::move(config), NAME);
}

SourceValidationRegistryReturnType RegisterProcessSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return ProcessSource::validateAndFormat(std::move(sourceConfig.config));
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
SourceRegistryReturnType SourceGeneratedRegistrar::RegisterProcessSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<ProcessSource>(sourceRegistryArguments.sourceDescriptor);
}

}
