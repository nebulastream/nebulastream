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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>

#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceReturnType.hpp>
#include <BackpressureChannel.hpp>

namespace NES
{

class TokioSource final
{
public:
    TokioSource(BackpressureListener listener, SourceDescriptor descriptor, OriginId originId);
    ~TokioSource();

    // Non-copyable, non-movable
    TokioSource(const TokioSource&) = delete;
    TokioSource& operator=(const TokioSource&) = delete;
    TokioSource(TokioSource&&) = delete;
    TokioSource& operator=(TokioSource&&) = delete;

    OriginId getOriginId();
    bool start(SourceReturnType::EmitFunction&& emitFunction) const;
    void stop() const;

    friend std::ostream& operator<<(std::ostream& out, const TokioSource& source);

private:
    struct Context;
    std::unique_ptr<Context> context;
    OriginId originId;
    SourceDescriptor descriptor;
    BackpressureListener backpressureHandler;
};

} // namespace NES
