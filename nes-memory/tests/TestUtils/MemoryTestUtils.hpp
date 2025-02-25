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
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Testing
{
/// Used for testing to copy buffers from the pipeline execution context, so that we can both free the original TupleBuffer and keep the copy of the TupleBuffer for later checks.
Memory::TupleBuffer copyBuffer(const Memory::TupleBuffer& buffer, Memory::AbstractBufferProvider& provider);
}
