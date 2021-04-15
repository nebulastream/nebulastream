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

#include <QueryCompiler/Compiler/SystemCompilerCompiledCode.hpp>
#include <Util/Logger.hpp>
#include <Util/SharedLibrary.hpp>
#include <iostream>
#include <filesystem>
namespace NES {

SystemCompilerCompiledCode::SystemCompilerCompiledCode(SharedLibraryPtr library, const std::string& baseName)
    : CompiledCode(), library(library), baseFileName(baseName){};
SystemCompilerCompiledCode::~SystemCompilerCompiledCode() { cleanUp(); }

void* SystemCompilerCompiledCode::getFunctionPointerImpl(const std::string& name) {
    NES_DEBUG("getFuncPtr for file" << name);
    return library->getSymbol(name);
}

void SystemCompilerCompiledCode::cleanUp() {
    NES_WARNING("delete all in folder=" << baseFileName);
    if (std::filesystem::exists(baseFileName + ".c")) {
        std::filesystem::remove(baseFileName + ".c");
    }
å
    if (std::filesystem::exists(baseFileName + ".o")) {
        std::filesystem::remove(baseFileName + ".o");
    }

    if (std::filesystem::exists(baseFileName + ".so")) {
        std::filesystem::remove(baseFileName + ".so");
    }

    if (std::filesystem::exists(baseFileName + ".cpp")) {
        std::filesystem::remove(baseFileName + ".cpp");
    }

    if (std::filesystem::exists(baseFileName + ".hpp")) {
        std::filesystem::remove(baseFileName + ".hpp");
    }

    if (std::filesystem::exists(baseFileName + ".c.orig")) {
        std::filesystem::remove(baseFileName + ".c.orig");
    }
}

CompiledCodePtr SystemCompilerCompiledCode::create(SharedLibraryPtr library, const std::string& baseName) {
    return std::make_shared<SystemCompilerCompiledCode>(library, baseName);
}
}// namespace NES