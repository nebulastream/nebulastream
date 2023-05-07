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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BABELFISH_BACKEND_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BABELFISH_BACKEND_HPP_
#include <Nautilus/Backends/CompilationBackend.hpp>
#include <memory>
#include <Util/JNI/JNI.hpp>
namespace NES::Nautilus::Backends::Babelfish {

class BabelfishCode;

/**
 * @brief Compilation backend that generates babelfish ir and compiles it at runtime.
 */
class BabelfishCompilationBackend : public CompilationBackend {
  public:
    /**
     * @brief Execution engine for babelfish code fragments.
     */
    class BabelfishEngine {
      public:
        BabelfishEngine();
        /**
         * @brief Deploys a new code fragment in the running engine and return an executable.
         * @param code
         * @return
         */
        std::unique_ptr<Executable> deploy(const BabelfishCode& code);
        ~BabelfishEngine();

      private:
        jni::jclass jniEndpoint;
    };
    BabelfishCompilationBackend();
    std::unique_ptr<Executable>
    compile(std::shared_ptr<IR::IRGraph> ir, const CompilationOptions& options, const DumpHelper& dumpHelper) override;

  private:
    std::unique_ptr<BabelfishEngine> engine = nullptr;
};

}// namespace NES::Nautilus::Backends::Babelfish
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BABELFISH_BACKEND_HPP_
