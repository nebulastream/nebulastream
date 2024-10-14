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

#include <string>
#ifndef INCLUDED_FROM_COMPILATION_BACKEND_REGISTRY
#    error "This file should not be included directly! " \
"Include instead include <Natuilus/Backends/CompilationBackendRegistry.hpp>"
#endif
namespace NES::Nautilus::Backends
{
std::unique_ptr<CompilationBackend> RegisterMlirBackend();

}
namespace NES
{
template <>
inline void Registrar<std::string, Nautilus::Backends::CompilationBackend>::registerAll([[maybe_unused]] Registry<Registrar>& registry)
{
#ifdef USE_MLIR
    registry.registerPlugin("MLIR", Nautilus::Backends::RegisterMlirBackend);
#endif
}
}
