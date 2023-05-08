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

#include <Util/Logger/Logger.hpp>
#include <Nautilus/Util/CompilationOptions.hpp>
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
    // Todo in issue #3709 we aim to remove the dependency on PROXY_FUNCTIONS_RESULT_DIR and replace it with a
    // configurable path (analog to 'dumpOutputPath'). There should be a default, so that it is not required to 
    // configure a path to use the proxyInlining flag (avoid dependency).
    if(proxyInlining && !std::filesystem::exists(PROXY_FUNCTIONS_RESULT_DIR)) {
        NES_THROW_RUNTIME_ERROR("We require a proxy functions file under: " << PROXY_FUNCTIONS_RESULT_DIR << 
                                " to perform proxy function inlining");
    }
    CompilationOptions::proxyInlining = proxyInlining; 
    CompilationOptions::proxyInliningPath = PROXY_FUNCTIONS_RESULT_DIR;
}
const std::string CompilationOptions::getProxyInliningPath() const { return proxyInliningPath; }

}// namespace NES::Nautilus
