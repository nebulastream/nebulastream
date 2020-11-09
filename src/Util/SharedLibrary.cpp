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

#include <Util/SharedLibrary.hpp>

#include <Util/Logger.hpp>
#include <assert.h>
#include <dlfcn.h>
#include <iostream>
#include <memory>

namespace NES {

SharedLibrary::SharedLibrary(void* _shared_lib)
    : shared_lib_(_shared_lib) {
    assert(shared_lib_ != NULL);
}

SharedLibrary::~SharedLibrary() {
    dlclose(shared_lib_);
}

void* SharedLibrary::getSymbol(const std::string& mangeled_symbol_name) const {
    auto symbol = dlsym(shared_lib_, mangeled_symbol_name.c_str());
    auto error = dlerror();

    if (error) {
        NES_ERROR(
            "Could not load symbol: " << mangeled_symbol_name << " Error:" << error);
        NES_THROW_RUNTIME_ERROR("Could not load symbol");
    }

    return symbol;
}

SharedLibraryPtr SharedLibrary::load(const std::string& file_path) {
    auto myso = dlopen(file_path.c_str(), RTLD_NOW);

    auto error = dlerror();
    if (error) {
        NES_ERROR(
            "Could not load shared library: " << file_path << " Error:" << error);
    } else if (!myso) {
        NES_ERROR(
            "Could not load shared library: " << file_path << "Error unknown!");
    }

    return SharedLibraryPtr(new SharedLibrary(myso));
}

}// namespace NES
