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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

#include <sys/types.h>

#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <PythonBatchWrapper.hpp>

/// What the implementations of the PythonProcess source share: starting and ending the processes, and where answers go.
namespace NES::PythonProcesses
{

/// The buffers the channel holds per process: more than a process takes in the retry interval of the sink. PyPy takes a batch in a
/// fraction of a millisecond, and 16 per process still left the processes idle three quarters of the time.
constexpr size_t BUFFERS_PER_PROCESS = 128;

/// How long a source waits for a buffer before it checks for a stop request.
constexpr auto BUFFER_WAIT = std::chrono::milliseconds(25);

/// The protocol of NES_UDF_PROCESS_PROTOCOL, which the worker sets from worker.python_udf.process_protocol.
[[nodiscard]] UdfProcessProtocol protocol();

/// NES_UDF_PROCESSES, which the worker sets from worker.python_udf.processes or its number of worker threads; 1 without it.
[[nodiscard]] size_t numberOfProcesses();

/// A Python process and the socket to it, which is its file descriptor 3.
struct Process
{
    pid_t pid = -1;
    int socket = -1;
};

/// Starts a Python process that runs `program`: the python of NES_UDF_PROCESS_VENV or else NES_UDF_VENV, otherwise python3 on the
/// PATH. `sharedFd` and `poolFd`, if not -1, become the file descriptors 4 and 5 of the process. Does not wait for the program to start.
/// @throws CannotLoadUdf if the process cannot be started
[[nodiscard]] Process start(std::string_view program, int sharedFd = -1, int poolFd = -1);

/// Waits for the status a process sends once its program started, see generatePythonBatchWrapper.
/// @throws CannotLoadUdf with the error of the process if it failed to start
void awaitStart(const Process& process);

/// Blocking, for the start of a process.
void sendAll(const Process& process, std::span<const std::byte> bytes);
void receiveAll(const Process& process, std::span<std::byte> bytes);

/// Closes the sockets, after which a process ends once it finished the batch it works on, and waits for the processes to end, killing
/// the ones that take too long.
void end(std::span<Process> processes);

/// Where the answer body of `bodySize` bytes goes in `output`: behind its header if it fits, otherwise into a child buffer.
[[nodiscard]] ArrowBufferRef::Location
placeBody(TupleBuffer& output, uint64_t bodySize, AbstractBufferProvider& bufferProvider, const ArrowBufferRef& layout);

}
