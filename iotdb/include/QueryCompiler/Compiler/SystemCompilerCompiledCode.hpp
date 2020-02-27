#ifndef NES_INCLUDE_QUERYCOMPILER_COMPILER_SYSTEMCOMPILERCOMPILEDCODE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_COMPILER_SYSTEMCOMPILERCOMPILEDCODE_HPP_
#include <Util/TimeMeasurement.hpp>
#include <QueryCompiler/Compiler/CompiledCode.hpp>

namespace NES{

class SharedLibrary;
typedef std::shared_ptr<SharedLibrary> SharedLibraryPtr;

class SystemCompilerCompiledCode : public CompiledCode {
 public:
  SystemCompilerCompiledCode(SharedLibraryPtr library,
                              const std::string &base_name);

  ~SystemCompilerCompiledCode() override;

 protected:
  void* getFunctionPointerImpl(const std::string &name) final;

 private:
  void cleanUp();

  SharedLibraryPtr library_;
  std::string base_file_name_;
};

CompiledCodePtr createSystemCompilerCompiledCode(SharedLibraryPtr library,
                                                 const std::string &base_name);

}

#endif //NES_INCLUDE_QUERYCOMPILER_COMPILER_SYSTEMCOMPILERCOMPILEDCODE_HPP_
