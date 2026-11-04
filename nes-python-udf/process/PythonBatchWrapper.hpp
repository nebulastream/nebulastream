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

/// Generates the Python program that a UDF process runs. The Arrow layout of the batches is baked into its code as offsets, so that
/// only buffers cross the socket. For every batch, it loops over the rows, reads the arguments out of the buffer, calls the UDFs
/// and writes the results into the columns the batch holds for them. It only needs the standard library.
///
/// All integers are little-endian uint64. With V the number of VARSIZED columns of the layout, in the order of their child slots:
///   start-up, process to worker: [status], then on a failure [message length][message]
///   batch, worker to process:    [number of rows][payload length of each of the V columns][buffer][the V payloads]
///   batch, process to worker:    [status], then either [message length][message] or [payload lengths][buffer][payloads]
/// Status is 1 on success and 0 on a failure.
[[nodiscard]] std::string generatePythonBatchWrapper(const ArrowBufferRef& layout, const std::vector<UdfProcessCall>& calls);

}
