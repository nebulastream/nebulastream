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

#include <Runtime/BufferManager.hpp>

namespace NES::ConnTest
{

/// A pool of at least `bufferCount` buffers of `bufferSize` bytes, which also hands out unpooled buffers up to a fixed budget.
std::shared_ptr<BufferManager> makeBufferPool(std::uint32_t bufferSize, std::uint32_t bufferCount);

}
