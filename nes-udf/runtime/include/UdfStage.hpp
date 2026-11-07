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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include <UdfAbi.h>
#include <UdfBackend.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

class InProcessFusedUdfs;

/// The UDFs of one UDF operator, all served by the same bridge library. They run as one unit per row or batch, under one acquisition
/// of whatever the bridge serializes on (the GIL for CPython) instead of one per UDF.
///
/// Memory layout of a row (all buffers are flat, the UDFs follow each other in the order they were passed in):
///   - `argValues`/`argLens`/`argNulls`: as in UdfBackend, with the arguments of all UDFs one after the other.
///   - `results`: one 8-byte slot per UDF. Receives a scalar result, or for a VARSIZED result a pointer to its bytes. They belong
///     to the calling thread and stay valid until its next call to executeRow, so the caller copies them out before then.
///   - `resultLens`: one 8-byte slot per UDF, receiving the length of a VARSIZED result.
///   - `resultNulls`: one byte per UDF, non-zero if its result is NULL.
class UdfStage
{
public:
    static constexpr std::size_t SLOT_BYTES = 8;

    /// With `fused`, the UDFs run in one call into the language per row or batch instead of one per UDF, see InProcessFusedUdfs.
    UdfStage(const std::vector<UdfDescriptor>& udfs, UdfInterpreter interpreter, bool fused = false);
    ~UdfStage();
    UdfStage(const UdfStage&) = delete;
    UdfStage& operator=(const UdfStage&) = delete;
    UdfStage(UdfStage&&) = delete;
    UdfStage& operator=(UdfStage&&) = delete;

    [[nodiscard]] bool isFused() const { return fused != nullptr; }

    /// Throws UdfExecutionError on a recoverable failure of a UDF.
    void executeRow(
        const std::int8_t* argValues,
        const std::int8_t* argLens,
        const std::int8_t* argNulls,
        std::int8_t* results,
        std::int8_t* resultLens,
        std::int8_t* resultNulls) const;

    /// Runs UDF `udf` on a batch, see UdfBackend::executeBatch. Throws UdfExecutionError on a recoverable failure of the UDF.
    void executeBatch(std::size_t udf, ArrowArray* arguments, ArrowArray* result) const;

    /// Runs all UDFs of a fused stage on a batch, see execute_udf_stage_batch: `arguments` holds the argument columns of all UDFs,
    /// `result` receives a struct array with the result column of each UDF. Throws UdfExecutionError on a recoverable failure.
    void executeFusedBatch(ArrowArray* arguments, ArrowArray* result) const;

private:
    struct Udf
    {
        std::shared_ptr<UdfBackend> backend;
        std::size_t firstArgument;
        bool varsizedResult;
    };

    std::vector<Udf> udfs;
    /// Declared after `udfs`, so it is destroyed before the UDFs it fuses.
    std::unique_ptr<InProcessFusedUdfs> fused;
};

}
