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
#include <Compiler/CPPCompiler/CUDAPlatform.hpp>
#include <Compiler/Exceptions/CompilerException.hpp>
#include <Util/Logger/Logger.hpp>
#include <filesystem>

namespace NES::Compiler {

CUDAPlatform::CUDAPlatform(const std::string& cudaSdkPath) : cudaSdkPath(cudaSdkPath) {
    if (cudaSdkPath.empty()) {
        NES_ERROR("CUDA SDK path is empty");
        throw CompilerException("CUDA SDK path is empty");
    }

    if (!std::filesystem::exists(cudaSdkPath)) {
        NES_ERROR("CUDA SDK path does not exist: {}", cudaSdkPath);
        throw CompilerException("CUDA SDK path does not exist: " + cudaSdkPath);
    }
}

const CompilerFlags CUDAPlatform::getCompilerFlags() const {
    // See https://www.llvm.org/docs/CompileCudaWithLLVM.html
    auto cudaFlags = {
        "--language=cuda",
        "--cuda-gpu-arch=native",
        "-lcudart",
        "-ldl",
        "-lrt",
        "-lpthread",
        "-Xclang -fcuda-allow-variadic-functions",
    };

    CompilerFlags flags;
    for (auto flag : cudaFlags) {
        flags.addFlag(flag);
    }
    flags.addFlag("--cuda-path=\"" + cudaSdkPath + "\"");
    flags.addFlag("-L\"" + cudaSdkPath + "\"/lib64");
    return flags;
}

}// namespace NES::Compiler
