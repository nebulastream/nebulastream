#include <QueryCompiler/Compiler/SystemCompilerCompiledCode.hpp>
#include <Util/SharedLibrary.hpp>
#include <boost/filesystem/operations.hpp>
#include <iostream>

namespace NES {

SystemCompilerCompiledCode::SystemCompilerCompiledCode(SharedLibraryPtr library,
                                                       const std::string& baseName) : CompiledCode(),
                                                                                      library(library),
                                                                                      baseFileName(baseName){};
SystemCompilerCompiledCode::~SystemCompilerCompiledCode() {
    cleanUp();
}

void* SystemCompilerCompiledCode::getFunctionPointerImpl(const std::string& name) {
    std::cout << baseFileName << std::endl;
    return library->getSymbol(name);
}

void SystemCompilerCompiledCode::cleanUp() {
    if (boost::filesystem::exists(baseFileName + ".c")) {
        boost::filesystem::remove(baseFileName + ".c");
    }

    if (boost::filesystem::exists(baseFileName + ".o")) {
        boost::filesystem::remove(baseFileName + ".o");
    }

    if (boost::filesystem::exists(baseFileName + ".so")) {
        boost::filesystem::remove(baseFileName + ".so");
    }

    if (boost::filesystem::exists(baseFileName + ".c.orig")) {
        boost::filesystem::remove(baseFileName + ".c.orig");
    }
}

CompiledCodePtr SystemCompilerCompiledCode::create(SharedLibraryPtr library, const std::string& baseName) {
    return std::make_shared<SystemCompilerCompiledCode>(library, baseName);
}
}// namespace NES