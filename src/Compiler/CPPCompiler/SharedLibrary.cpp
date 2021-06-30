#include <Compiler/CPPCompiler/SharedLibrary.hpp>
#include <Compiler/Exceptions/CompilerException.hpp>
#include <Util/Logger.hpp>
#include <dlfcn.h>
namespace NES::Compiler {

SharedLibrary::SharedLibrary(void* shareLib) : shareLib(shareLib) { NES_ASSERT(shareLib != nullptr, "Shared lib is null"); }

SharedLibrary::~SharedLibrary() {
    NES_DEBUG("~SharedLibrary()");
    auto returnCode = dlclose(shareLib);
    if (returnCode != 0) {
        NES_ERROR("SharedLibrary: error during dlclose. error code:" << returnCode);
    }
    NES_DEBUG("SharedLibrary: !!!! We currently dont unload shared libs from memory !!!!");
}

void* SharedLibrary::getSymbol(const std::string& mangeldSymbolName) const {
    auto* symbol = dlsym(shareLib, mangeldSymbolName.c_str());
    auto* error = dlerror();

    if (error) {
        NES_ERROR("Could not load symbol: " << mangeldSymbolName << " Error:" << error);
        throw CompilerException("Could not load symbol: " + mangeldSymbolName + " Error:" + error);
    }

    return symbol;
}

SharedLibraryPtr SharedLibrary::load(const std::string& filePath) {
    auto* shareLib = dlopen(filePath.c_str(), RTLD_NOW);
    auto* error = dlerror();
    if (error) {
        NES_ERROR("Could not load shared library: " << filePath << " Error:" << error);
        throw CompilerException("Could not load shared library: " + filePath + " Error:" + error);
    }
    if (!shareLib) {
        NES_ERROR("Could not load shared library: " << filePath << "Error unknown!");
        throw CompilerException("Could not load shared library: " + filePath);
    }

    return std::make_shared<SharedLibrary>(shareLib);
}

}// namespace NES::Compiler