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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_COMPILATIONBACKEND_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_COMPILATIONBACKEND_HPP_
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/Util/CompilationOptions.hpp>
#include <Util/DumpHelper.hpp>
#include <Util/PluginRegistry.hpp>

namespace NES::Nautilus::Backends {
class Executable;

/**
 * @brief The compilation backend, compiles a ir graph to an executable.
 */
class CompilationBackend {
  public:
    /**
     * @brief Compiles ir graph to executable.
     * @return std::unique_ptr<Executable>
     */
    virtual std::unique_ptr<Executable>
    compile(std::shared_ptr<IR::IRGraph>, const CompilationOptions& options, const DumpHelper& dumpHelper) = 0;
    virtual ~CompilationBackend() = default;
};

using CompilationBackendRegistry = Util::NamedPluginRegistry<CompilationBackend>;

}// namespace NES::Nautilus::Backends

#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_COMPILATIONBACKEND_HPP_
