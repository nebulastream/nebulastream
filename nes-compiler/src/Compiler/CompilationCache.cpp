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

bool CompilationCache::exists(std::shared_ptr<SourceCode> code) {
    std::unique_lock lock(mutex);
    return compilationReuseMap.contains(code);
};

void CompilationCache::insert(std::pair<std::shared_ptr<SourceCode>, CompilationResult> newEntry) {
    std::unique_lock lock(mutex);
    if (exists(newEntry.first)) {
        NES_WARNING("Compilation Cache: inserted item already exists");
    } else {
        compilationReuseMap.insert(newEntry);
    }
}

CompilationResult CompilationCache::get(std::shared_ptr<SourceCode> code) {
    std::unique_lock lock(mutex);
    return compilationReuseMap.find(code)->second;
}
}// namespace NES::Compiler