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

/// Parses a JSON document with simdjson and returns it as a flat, pre-order node list.
///
/// Every node occupies five int64 slots in `nodeInts` (kind, count, payload, textOffset, textSize) and one double in
/// `nodeReals`. `count` is the number of direct children: array elements, or object pairs. For each object pair the
/// key is emitted as a String node immediately before its value node. Strings and keys are decoded into `text`.
///
/// Node kinds: 0 null, 1 bool (payload 0/1), 2 int (payload), 3 float (nodeReals), 4 string (text slice), 5 array, 6 object.
/// Returns 0 on success; otherwise a message is written to `errorPointer`.
extern "C" uint8_t nes_json_parse(
    const int8_t* inputPointer,
    uint64_t inputSize,
    int64_t** nodeIntsPointer,
    double** nodeRealsPointer,
    uint64_t* nodeCount,
    int8_t** textPointer,
    uint64_t* textSize,
    int8_t** errorPointer,
    uint64_t* errorSize);
