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

#include <Compiler/CompilationCache.hpp>
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/SourceCode.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES::Compiler {

bool CompilationCache::contains(const SourceCode& code) {
    std::lock_guard<std::recursive_mutex> lk(mutex);
    return compilationReuseMap.contains(code);
};

void CompilationCache::insert(const SourceCode& code, CompilationResult& newEntry) {
    std::lock_guard<std::recursive_mutex> lk(mutex);
    if (contains(code)) {
        NES_WARNING("Compilation Cache: inserted item already exists");
    } else {
        compilationReuseMap.insert(std::make_pair(code, newEntry));
    }
}

CompilationResult CompilationCache::get(const SourceCode& code) {
    std::lock_guard<std::recursive_mutex> lk(mutex);
    return compilationReuseMap.find(code)->second;
}
}// namespace NES::Compiler
