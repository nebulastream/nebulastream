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

#include <memory>
#include <vector>

#include <UdfAbi.h>
#include <UdfInterpreter.hpp>

namespace NES
{

class UdfDescriptor;

/// The seam between the engine and however a scalar UDF is actually loaded and run: InProcessBackend dlopens a bridge in the
/// worker. A UDF runs per row or on batches of rows, whose argument and result columns are Arrow arrays.
///
/// Argument marshalling of a row: `argValues` is a flat buffer of `argc` 8-byte slots -- the value itself for a fixed-width
/// argument, or a pointer to raw bytes (length in the matching `argLens` slot) for VARSIZED. `argNulls` is `argc` bytes; non-zero
/// marks the argument SQL NULL.
class UdfBackend
{
public:
    UdfBackend() = default;
    virtual ~UdfBackend() = default;
    UdfBackend(const UdfBackend&) = delete;
    UdfBackend& operator=(const UdfBackend&) = delete;
    UdfBackend(UdfBackend&&) = delete;
    UdfBackend& operator=(UdfBackend&&) = delete;

    /// Execute a UDF with a fixed-width scalar return. Writes the 8-byte scalar result into
    /// `resultScalar` and sets `*resultNull`. Throws UdfExecutionError on a recoverable failure.
    virtual void executeScalarRow(
        const std::int8_t* argValues, const std::int8_t* argLens, const std::int8_t* argNulls, std::int8_t* resultScalar, int* resultNull)
        = 0;

    /// Execute a UDF with a VARSIZED return. Replaces the content of `result` with the result bytes (none for NULL) and sets
    /// `*resultNull`. Throws UdfExecutionError on a recoverable failure.
    virtual void executeVarsizedRow(
        const std::int8_t* argValues,
        const std::int8_t* argLens,
        const std::int8_t* argNulls,
        std::vector<std::int8_t>& result,
        int* resultNull) = 0;

    /// Runs the UDF on a batch, see execute_udf_batch in UdfAbi.h: takes ownership of `arguments` and, on success, sets
    /// `result`, which the caller then owns. Throws UdfExecutionError on a recoverable failure.
    virtual void executeBatch(ArrowArray* arguments, ArrowArray* result) = 0;

    /// Bracket the calls for one row or batch, see enter_udf_row in UdfAbi.h.
    [[nodiscard]] virtual int enterRow() = 0;
    virtual void exitRow(int state) = 0;

    /// Load and prepare the UDF described by `descriptor`, from its entry point or its inline source, to run in `interpreter`.
    /// v1 always returns an in-process backend.
    [[nodiscard]] static std::shared_ptr<UdfBackend> create(const UdfDescriptor& descriptor, UdfInterpreter interpreter);
};

}
