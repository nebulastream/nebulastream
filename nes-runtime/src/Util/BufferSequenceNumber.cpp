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

#include <Util/BufferSequenceNumber.hpp>

namespace NES {
uint64_t BufferSequenceNumber::getSequenceNumber() const { return sequenceNumber; }

uint64_t BufferSequenceNumber::getOriginId() const { return originId; }

}// namespace NES
