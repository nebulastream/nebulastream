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

#include <Nautilus/Util/CompilationOptions.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <filesystem>
#include <string>

namespace NES::Nautilus {

const std::string CompilationOptions::getIdentifier() const { return identifier; }
void CompilationOptions::setIdentifier(const std::string& identifier) { CompilationOptions::identifier = identifier; }
const std::string CompilationOptions::getDumpOutputPath() const { return dumpOutputPath; }
void CompilationOptions::setDumpOutputPath(const std::string& dumpOutputPath) {
    CompilationOptions::dumpOutputPath = dumpOutputPath;
}
bool CompilationOptions::isDumpToFile() const { return dumpToFile; }
void CompilationOptions::setDumpToFile(bool dumpToFile) { CompilationOptions::dumpToFile = dumpToFile; }
bool CompilationOptions::isDumpToConsole() const { return dumpToConsole; }
void CompilationOptions::setDumpToConsole(bool dumpToConsole) { CompilationOptions::dumpToConsole = dumpToConsole; }
bool CompilationOptions::isOptimize() const { return optimize; }
void CompilationOptions::setOptimize(bool optimize) { CompilationOptions::optimize = optimize; }
bool CompilationOptions::isDebug() const { return debug; }
void CompilationOptions::setDebug(bool debug) { CompilationOptions::debug = debug; }
bool CompilationOptions::isProxyInlining() const { return proxyInlining; }
void CompilationOptions::setProxyInlining(const bool proxyInlining) {
    // For now we statically write proxy functions to /tmp/proxiesReduced.ll (see ExtractFunctionFromLLVMIR.cpp)
    CompilationOptions::proxyInliningInputPath = std::filesystem::temp_directory_path().string() + "/proxiesReduced.ll";
    if (proxyInlining && !std::filesystem::exists(proxyInliningInputPath)) {
        NES_THROW_RUNTIME_ERROR("We require a proxy functions file under: " << proxyInliningInputPath
                                                                            << " to perform proxy function inlining");
    }
    CompilationOptions::proxyInlining = proxyInlining;
}
const std::string CompilationOptions::getProxyInliningInputPath() const { return proxyInliningInputPath; }
void CompilationOptions::setOptimizationLevel(const uint8_t optimizationLevel) {
    CompilationOptions::optimizationLevel = optimizationLevel;
};
uint8_t CompilationOptions::getOptimizationLevel() const { return optimizationLevel; };

void CompilationOptions::useCUDA(bool cuda) { CompilationOptions::cuda = cuda; }
bool CompilationOptions::usingCUDA() const { return cuda; }
void CompilationOptions::setCUDASdkPath(const std::string& cudaSdkPath) { CompilationOptions::cudaSdkPath = cudaSdkPath; }
const std::string CompilationOptions::getCUDASdkPath() const { return cudaSdkPath; }

}// namespace NES::Nautilus
