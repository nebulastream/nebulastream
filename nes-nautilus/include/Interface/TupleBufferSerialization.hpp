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

#include <istream>
#include <ostream>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{

/// Writes a TupleBuffer together with all of its nested child buffers. Only the payload, the number of tuples and the order of the
/// children are kept. Child buffer indices stored inside the payload therefore stay valid across a round trip, raw pointers do not.
void serializeTupleBufferTree(std::ostream& out, const TupleBuffer& buffer);

/// Recreates a buffer tree written by serializeTupleBufferTree() in unpooled buffers of the original sizes.
[[nodiscard]] TupleBuffer deserializeTupleBufferTree(std::istream& in, AbstractBufferProvider& bufferProvider);

}
