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
