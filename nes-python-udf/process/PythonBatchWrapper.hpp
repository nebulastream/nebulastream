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

#include <string>
#include <vector>

#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <UdfProcessCall.hpp>

namespace NES
{

/// Generates the Python program that a UDF process runs, on pyarrow. For every record batch, it converts the argument columns to
/// Python values, calls each UDF once per row, and builds the result columns as Arrow arrays, which replace the columns the batch
/// holds for them. The columns are addressed by their position in the layout.
///
/// The worker sends an Arrow IPC stream: the schema, then one record batch per buffer. The process answers each with a status, a
/// little-endian uint64 that is 1 on success and 0 on a failure, followed on a success by the next message of its own IPC stream,
/// whose first batch is preceded by the same schema, and on a failure by a uint64 length and an error message.
///   start-up, process to worker: [status], then on a failure [message length][message]
[[nodiscard]] std::string generatePythonBatchWrapper(const ArrowBufferRef& layout, const std::vector<UdfProcessCall>& calls);

}
