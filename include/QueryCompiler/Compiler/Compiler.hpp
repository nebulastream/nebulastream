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
    /**
     * @brief Create a new compiler instance.
     * The object can be reused to compile code of multiple source files.
     * @return CompilerPtr
     */
    static CompilerPtr create();

    /**
     * @brief Compiles the source string.
     * @param source
     * @param debugging flag indicates if we should use debugging options for compiling this code.
     * This impacts the compiler flags and if the code is formatted and printed to the console
     * @return CompiledCode
     */
    CompiledCodePtr compile(const std::string& source, bool debugging);

  private:
    /**
     * @brief create a unique file name in the following structure: gen_query_%d-%m-%Y_%H-%M-%S
     * @return file name
     */
    std::string getFileName();

    /**
     * @brief Calls the system compiler with a set of compiler flags.
     * @param args CompilerFlagsPtr
     */
    void callSystemCompiler(CompilerFlagsPtr args);

    /**
     * @brief Helper function to write source code to a file
     * @param filename
     * @param source
     */
    void writeSourceToFile(const std::string& filename, const std::string& source);

    /**
     * @brief Helper function to format and print a source file using clang-format. This function is creating a temp file.
     * @param filename
     */
    std::string formatAndPrintSource(const std::string& filename);

    const static std::string IncludePath;
};

}// namespace NES

#endif// INCLUDE_CODE_COMPILER_HPP_
