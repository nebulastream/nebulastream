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
#include <Compiler/Exceptions/CompilerException.hpp>
#include <Compiler/Util/SharedLibrary.hpp>
#include <Util/Logger.hpp>
#include <dlfcn.h>
#include <filesystem>

namespace NES::Compiler {

SharedLibrary::SharedLibrary(void* shareLib, std::string soAbsolutePath)
    : DynamicObject(), shareLib(shareLib), soAbsolutePath(soAbsolutePath) {
    NES_ASSERT(shareLib != nullptr, "Shared lib is null");
}

SharedLibrary::~SharedLibrary() {
    NES_DEBUG("~SharedLibrary()");
    auto returnCode = dlclose(shareLib);
    if (returnCode != 0) {
        NES_ERROR("SharedLibrary: error during dlclose. error code:" << returnCode);
    }
    std::filesystem::remove(soAbsolutePath);
}

SharedLibraryPtr SharedLibrary::load(const std::string& absoluteFilePath) {
    auto* shareLib = dlopen(absoluteFilePath.c_str(), RTLD_NOW);
    auto* error = dlerror();
    if (error) {
        NES_ERROR("Could not load shared library: " << absoluteFilePath << " Error:" << error);
        throw CompilerException("Could not load shared library: " + absoluteFilePath + " Error:" + error);
    }
    if (!shareLib) {
        NES_ERROR("Could not load shared library: " << absoluteFilePath << "Error unknown!");
        throw CompilerException("Could not load shared library: " + absoluteFilePath);
    }

    return std::make_shared<SharedLibrary>(shareLib, absoluteFilePath);
}
void* SharedLibrary::getInvocableFunctionPtr(const std::string& mangeldSymbolName) {
    auto* symbol = dlsym(shareLib, mangeldSymbolName.c_str());
    auto* error = dlerror();

    if (error) {
        NES_ERROR("Could not load symbol: " << mangeldSymbolName << " Error:" << error);
        throw CompilerException("Could not load symbol: " + mangeldSymbolName + " Error:" + error);
    }

    return symbol;
}

}// namespace NES::Compiler