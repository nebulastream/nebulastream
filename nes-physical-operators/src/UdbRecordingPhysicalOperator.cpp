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

#include <UdbRecordingPhysicalOperator.hpp>

#include <cstdlib>
#include <fcntl.h>
#include <optional>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <CompilationContext.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>
#include <function.hpp>

namespace NES
{

namespace
{

/// Spawns udb attached to the current NES PID. udb detaches itself after attaching so no reaping needed.
///
/// Prerequisites:
///   1. UDB_BINARY_PATH must point to the udb executable, e.g. via direnv:
///        export UDB_BINARY_PATH=/path/to/udb
///   2. ptrace_scope must be 0 on Linux, otherwise user-space processes are not allowed to attach.
///      Check with: cat /proc/sys/kernel/yama/ptrace_scope
///      Allow for the current session with: echo 0 | sudo tee /proc/sys/kernel/yama/ptrace_scope
void spawnUdbProxy(const char* traceName)
{
    const char* udbBin = std::getenv("UDB_BINARY_PATH");
    if (udbBin == nullptr)
    {
        NES_WARNING("UDB_BINARY_PATH is not set — skipping udb recording");
        return;
    }

    /// Build strings before fork() — malloc is not async-signal-safe in the child.
    char pidBuf[32];
    ::snprintf(pidBuf, sizeof(pidBuf), "%d", static_cast<int>(::getpid()));

    char traceFileBuf[512];
    if (traceName != nullptr)
        ::snprintf(traceFileBuf, sizeof(traceFileBuf), "%s.undo", traceName);

    NES_DEBUG("UdbRecordingPhysicalOperator: spawning udb (binary={}, pid={})", udbBin, pidBuf);

    /// Pipe with O_CLOEXEC on the write end: exec closes it automatically on success.
    /// If execlp fails the child writes a byte so the parent can log the error safely.
    int pipeFd[2];
    if (::pipe2(pipeFd, O_CLOEXEC) != 0)
    {
        NES_ERROR("UdbRecordingPhysicalOperator: pipe2 failed");
        return;
    }

    const pid_t child = ::fork();
    if (child == 0)
    {
        /// Child: write end is O_CLOEXEC — exec closes it. On failure write a byte so parent detects it.
        if (traceName != nullptr)
            ::execlp(udbBin, udbBin, "--pid", pidBuf, "--recording-file", traceFileBuf, nullptr);
        else
            ::execlp(udbBin, udbBin, "--pid", pidBuf, nullptr);

        /// execlp only returns on failure — only async-signal-safe calls allowed here.
        const char errByte = 1;
        static_cast<void>(::write(pipeFd[1], &errByte, 1));
        ::_exit(127);
    }

    /// Parent: close write end, then check if child signalled failure.
    ::close(pipeFd[1]);

    if (child < 0)
    {
        NES_ERROR("UdbRecordingPhysicalOperator: fork failed");
        ::close(pipeFd[0]);
        return;
    }

    char result = 0;
    if (::read(pipeFd[0], &result, 1) == 1)
    {
        NES_ERROR("UdbRecordingPhysicalOperator: execlp failed for binary '{}'", udbBin);
        ::waitpid(child, nullptr, 0);
    }
    ::close(pipeFd[0]);
}

}

UdbRecordingPhysicalOperator::UdbRecordingPhysicalOperator(std::optional<std::string> traceName)
    : traceName(std::move(traceName))
{
}

void UdbRecordingPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    if (child.has_value())
    {
        setupChild(executionCtx, compilationContext);
    }
    const char* traceNamePtr = traceName.has_value() ? traceName->c_str() : nullptr;
    nautilus::invoke(spawnUdbProxy, nautilus::val<const char*>(traceNamePtr));
}

void UdbRecordingPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    if (child.has_value())
    {
        openChild(executionCtx, recordBuffer);
    }
}

void UdbRecordingPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    if (child.has_value())
    {
        executeChild(executionCtx, record);
    }
}

void UdbRecordingPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    if (child.has_value())
    {
        closeChild(executionCtx, recordBuffer);
    }
}

void UdbRecordingPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    if (child.has_value())
    {
        terminateChild(executionCtx);
    }
}

std::optional<PhysicalOperator> UdbRecordingPhysicalOperator::getChild() const
{
    return child;
}

void UdbRecordingPhysicalOperator::setChild(PhysicalOperator newChild)
{
    child = std::move(newChild);
}

}
