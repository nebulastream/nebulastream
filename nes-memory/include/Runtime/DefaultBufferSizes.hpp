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

namespace NES
{

/// Identifies a component that requests default-sized buffers from a buffer provider.
/// The actual sizes/policies live in DefaultBufferSizes.cpp - tweak them there without
/// recompiling every includer of this header.
/// Keep NUM_COMPONENTS last: it sizes the policy and state tables in the .cpp.
enum class BufferComponent : uint8_t
{
    ARENA_PAGE,
    PAGED_VECTOR_VAR_SIZED,
    OUTPUT_FORMATTER_CHILD,
    EMIT_OUTPUT,
    SOURCE_OUTPUT,
    NUM_COMPONENTS,
};

/// Returns the default buffer size (in bytes) a given component should request. Thread safe.
size_t getDefaultBufferSize(BufferComponent component);

}
