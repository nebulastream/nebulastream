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
#include <memory>

namespace NES
{

class UdfDescriptor;

/// The seam between the engine and however a scalar UDF is actually loaded and run.
///
/// v1 provides InProcessBackend (dlopen in the worker). A future SidecarBackend can run the UDF in
/// a separate process to contain fatal crashes, satisfying this same interface with no change to the
/// physical function, lowering, catalog, or DDL layers.
///
/// Argument marshalling (both entry points): `argValues` is a flat buffer of `argc` 8-byte slots. For
/// a fixed-width argument the slot holds the value; for a VARSIZED argument it holds a pointer to the
/// raw bytes, whose length is the matching 8-byte slot in `argLens`. `argNulls` is `argc` bytes; a
/// non-zero byte marks the argument SQL NULL.
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

    /// Execute a UDF with a VARSIZED return. Copies the result bytes into `resultBuffer` (at most
    /// `maxResultLen`), returns the result length, and sets `*resultNull`. Throws UdfExecutionError on
    /// a recoverable failure or if the result exceeds `maxResultLen`.
    virtual std::uint64_t executeVarsizedRow(
        const std::int8_t* argValues,
        const std::int8_t* argLens,
        const std::int8_t* argNulls,
        std::int8_t* resultBuffer,
        std::uint64_t maxResultLen,
        int* resultNull)
        = 0;

    /// Load and prepare the UDF described by `descriptor`. v1 always returns an in-process backend.
    [[nodiscard]] static std::shared_ptr<UdfBackend> create(const UdfDescriptor& descriptor);
};

}
