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

#ifndef NES_NAUTILUSBACKEND_HPP
#define NES_NAUTILUSBACKEND_HPP

#include <cstdint>

namespace NES::QueryCompilation {
enum class NautilusBackend : uint8_t {
    // Uses the interpretation based nautilus backend.
    INTERPRETER,
    // Uses the mlir based nautilus backend.
    MLIR_COMPILER_BACKEND,
    // Uses the byte code interpretation based nautilus backend.
    BC_INTERPRETER_BACKEND,
    // Uses the flounder based nautilus backend.
    FLOUNDER_COMPILER_BACKEND,
    // Uses the cpp based nautilus backend.
    CPP_COMPILER_BACKEND
};
}
#endif//NES_NAUTILUSBACKEND_HPP
