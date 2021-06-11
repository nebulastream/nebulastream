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

#ifndef INCLUDE_CODE_COMPILER_HPP_
#define INCLUDE_CODE_COMPILER_HPP_

#include <Util/TimeMeasurement.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES {

class SharedLibrary;
using SharedLibraryPtr = std::shared_ptr<SharedLibrary>;

class CompiledCode;
using CompiledCodePtr = std::shared_ptr<CompiledCode>;

class Compiler;
using CompilerPtr = std::shared_ptr<Compiler>;

class CompilerFlags;
using CompilerFlagsPtr = std::shared_ptr<CompilerFlags>;

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
     * @param compilerMode
     * This impacts the compiler flags and if the code is formatted and printed to the console
     * @return CompiledCode
     */
    CompiledCodePtr compile(const std::string& source);

  private:
    /**
     * @brief create a unique file name in the following structure: gen_query_%d-%m-%Y_%H-%M-%S
     * @return file name
     */
    static std::string getFileName();

    /**
     * @brief Calls the system compiler with a set of compiler flags.
     * @param args CompilerFlagsPtr
     */
    static void callSystemCompiler(CompilerFlagsPtr flags, std::string const& filename);

    /**
     * @brief Helper function to write source code to a file
     * @param filename
     * @param source
     */
    static void writeSourceToFile(const std::string& filename, const std::string& source);

    /**
     * @brief Helper function to format and print a source file using clang-format. This function is creating a temp file.
     * @param filename
     */
    static std::string formatAndPrintSource(const std::string& filename);

    const static std::string NESIncludePath;
    const static std::string DEBSIncludePath;
};

}// namespace NES

#endif// INCLUDE_CODE_COMPILER_HPP_
