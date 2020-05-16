#ifndef INCLUDE_CODE_COMPILER_HPP_
#define INCLUDE_CODE_COMPILER_HPP_

#include <Util/TimeMeasurement.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES {

class SharedLibrary;
typedef std::shared_ptr<SharedLibrary> SharedLibraryPtr;

class CompiledCode;
typedef std::shared_ptr<CompiledCode> CompiledCodePtr;

class Compiler;
typedef std::shared_ptr<Compiler> CompilerPtr;

class CompilerFlags;
typedef std::shared_ptr<CompilerFlags> CompilerFlagsPtr;

/**
 * @brief The Compiler compiles a source code fragment to a dynamic library.
 */
class Compiler {
  public:
    CompilerPtr create();

    /**
     * @brief Compiles the source string.
     * @param source
     * @param debugging flag indicates if we should use debugging options for compiling this code.
     * @return CompiledCode
     */
    CompiledCodePtr compile(const std::string& source, bool debugging);

  private:
    std::string getFileName();
    void callSystemCompiler(CompilerFlagsPtr args);

    void writeSourceToFile(const std::string& filename, const std::string& source);
    void formatAndPrintSource(const std::string& source);

    const static std::string IncludePath;
};

}// namespace NES

#endif// INCLUDE_CODE_COMPILER_HPP_
