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
#include <optional>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{

enum class UnpooledBufferManagerType : uint8_t
{
    CHUNKED,
    MALLOC
};

class UnpooledBufferManager
{
public:
    virtual ~UnpooledBufferManager() = default;
    [[nodiscard]] virtual size_t getNumberOfUnpooledBuffers() const = 0;
    virtual std::optional<TupleBuffer>
    getUnpooledBuffer(size_t neededSize, size_t alignment, const std::shared_ptr<BufferRecycler>& bufferRecycler) = 0;
};

}
