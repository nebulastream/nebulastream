#include <boost/filesystem/operations.hpp>
#include <QueryCompiler/Compiler/SystemCompilerCompiledCode.hpp>
#include <Util/SharedLibrary.hpp>

namespace NES {

SystemCompilerCompiledCode::SystemCompilerCompiledCode(SharedLibraryPtr library,
                                                       const std::string &base_name) :
    CompiledCode(),
    library(library),
    base_file_name_(base_name) {};
SystemCompilerCompiledCode::~SystemCompilerCompiledCode() {
  cleanUp();
}

void *SystemCompilerCompiledCode::getFunctionPointerImpl(const std::string &name) {
  return library->getSymbol(name);
}

void SystemCompilerCompiledCode::cleanUp() {
  if (boost::filesystem::exists(base_file_name_ + ".c")) {
    boost::filesystem::remove(base_file_name_ + ".c");
  }

  if (boost::filesystem::exists(base_file_name_ + ".o")) {
    boost::filesystem::remove(base_file_name_ + ".o");
  }

  if (boost::filesystem::exists(base_file_name_ + ".so")) {
    boost::filesystem::remove(base_file_name_ + ".so");
  }

  if (boost::filesystem::exists(base_file_name_ + ".c.orig")) {
    boost::filesystem::remove(base_file_name_ + ".c.orig");
  }
}

CompiledCodePtr createSystemCompilerCompiledCode(SharedLibraryPtr library,
                                                 const std::string &base_name) {
  return std::make_shared<SystemCompilerCompiledCode>(library, base_name);
}
}