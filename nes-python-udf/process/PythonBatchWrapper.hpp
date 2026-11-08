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

#include <cstdint>
#include <string>
#include <vector>

#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <UdfProcessCall.hpp>

namespace NES
{

/// How a UDF process and the worker exchange record batches.
enum class UdfProcessProtocol : uint8_t
{
    /// Arrow IPC streams, which the process reads and writes with pyarrow.
    ARROW_IPC,
    /// The Arrow buffers of the columns, framed by their sizes, which the process reads and writes in plain Python, so that it runs on
    /// any Python, PyPy included, without extension modules.
    RAW_BUFFERS,
    /// Batches framed like RAW_BUFFERS, in ring buffers in memory the worker and the process share, without system calls while both
    /// are busy.
    SHARED_MEMORY,
    /// Like SHARED_MEMORY, but without copying the batches: the buffer pool of the worker is shared memory the process maps, and the
    /// rings only carry where in it the columns are. The process writes its results into their columns, which the batches hold empty,
    /// and VARSIZED values into buffers of the pool the worker gives it ahead of time, so that the batches go on as they are.
    SHARED_MEMORY_ZERO_COPY,
};

/// Generates the Python program that a UDF process runs. For every record batch, it converts the argument columns to Python values,
/// calls each UDF once per row, and builds the result columns, which replace the columns the batch holds for them. The columns are
/// addressed by their position in the layout.
///
/// Every answer starts with a status, a little-endian uint64 that is 1 on success and 0 on a failure, followed on a failure by a uint64
/// length and an error message.
///   start-up, process to worker: [status], then on a failure [message length][message]
/// With ARROW_IPC, the worker sends an Arrow IPC stream: the schema, then one record batch per buffer. On a success, the status is
/// followed by the next message of the process's own IPC stream, whose first batch is preceded by the same schema.
/// With RAW_BUFFERS, a batch is [rows][body size][body], both as little-endian uint64, in both directions, the answer after the
/// status. The body holds, for each field in the order of the layout, its Arrow buffers as [size][bytes, padded to 8 bytes]: validity
/// and data, and for VARSIZED validity, int32 offsets and values. An empty validity buffer means that no value is NULL.
/// With SHARED_MEMORY, the worker passes a shared memory file as file descriptor 4: a control block of 4096 bytes, then a ring of
/// requests and a ring of answers of equal size. Each ring holds records, 8-byte aligned and contiguous; a record size of 2^64-1 means
/// that the next record starts at the beginning of the ring. A request is [record size][rows][body size][body], an answer
/// [record size][status][rows][body size][body] or, on a failure, [record size][0][message length][0][message]. The control block holds,
/// as uint64 on cache lines of their own, how many bytes of each ring the producer published and the consumer took, and whether the
/// process or the worker sleeps, see PythonProcessShmSource.
[[nodiscard]] std::string
generatePythonBatchWrapper(const ArrowBufferRef& layout, const std::vector<UdfProcessCall>& calls, UdfProcessProtocol protocol);

}
