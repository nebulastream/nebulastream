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
#include <ostream>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include "QueryId.hpp"

namespace NES
{

struct SourceRuntimeConfiguration
{
    size_t inflightBufferLimit;
};

/// Abstract interface for source lifecycle management.
/// Concrete implementations: SourceThreadHandle (C++ sources) and AsyncSourceHandle (Tokio/Rust sources).
class SourceHandle
{
public:
    virtual ~SourceHandle() = default;

    virtual bool start(QueryId queryId, SourceReturnType::EmitFunction&& emitFunction, SourceReturnType::AsyncEmitFunction&& asyncEmitFunction) = 0;
    virtual void stop() = 0;
    [[nodiscard]] virtual SourceReturnType::TryStopResult tryStop(std::chrono::milliseconds timeout) = 0;
    [[nodiscard]] virtual OriginId getSourceId() const = 0;
    [[nodiscard]] virtual const SourceRuntimeConfiguration& getRuntimeConfiguration() const = 0;

    friend std::ostream& operator<<(std::ostream& out, const SourceHandle& sourceHandle)
    {
        return sourceHandle.toString(out);
    }

protected:
    virtual std::ostream& toString(std::ostream& os) const = 0;
};

}

FMT_OSTREAM(NES::SourceHandle);
