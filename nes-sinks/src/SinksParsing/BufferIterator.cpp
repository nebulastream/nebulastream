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

#include <SinksParsing/BufferIterator.hpp>

#include <algorithm>
#include <cstddef>
#include <Runtime/VariableSizedAccess.hpp>

namespace NES
{

BufferIterator::BufferElement BufferIterator::getNextElement()
{
    /// The very first buffer to be returned as element is the "main" tuple buffer
    /// the number of bytes in all children (see OutputFormatterRef). In this case the whole tupĺe buffer is filled.
    if (bufferIndex == 0)
    {
        const size_t bytesInBuffer = tupleBuffer.getNumberOfTuples();
        const bool isLast = bufferIndex == tupleBuffer.getNumberOfChildBuffers();
        bufferIndex++;
        return {tupleBuffer, bytesInBuffer, isLast};
    }

    /// Iterate through the children
    /// Child buffers store the number of written bytes in them as the number of tuples
    const TupleBuffer childBuffer = tupleBuffer.loadChildBuffer(VariableSizedAccess::Index(bufferIndex - 1));
    const bool isLast = bufferIndex == tupleBuffer.getNumberOfChildBuffers();
    bufferIndex++;
    return {childBuffer, childBuffer.getNumberOfTuples(), isLast};
}
}
