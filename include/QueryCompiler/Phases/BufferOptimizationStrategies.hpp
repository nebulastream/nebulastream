/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef BUFFEROPTIMIZATIONSTRATEGIES_HPP
#define BUFFEROPTIMIZATIONSTRATEGIES_HPP

namespace NES {

enum BufferOptimizationStrategy {
    DEFAULT, // create seperate result buffer and copy everything over after all operations are applied. Check size after every written tuple.
    REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK, // Output schema is smaller or equal (bytes) than input schema. We can reuse the buffer and omit size checks.
    REUSE_INPUT_BUFFER, // enable the two optimizations individually (benchmarking only)
    OMIT_OVERFLOW_CHECK,
    ONLY_INPLACE_OPERATIONS // If all records and all fields match up in input and result buffer we can simply emit the input buffer. For this no filter can be applied and no new fields can be added. The only typical operations possible are inplace-maps, e.g. "id = id + 1".
};

//const std::string toString(const UnaryOperatorType& type);

}// namespace NES

#endif//BUFFEROPTIMIZATIONSTRATEGIES_HPP
