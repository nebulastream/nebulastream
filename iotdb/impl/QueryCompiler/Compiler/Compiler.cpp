#include <QueryCompiler/Compiler/CompiledCode.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <QueryCompiler/Compiler/CompilerFlags.hpp>
#include <QueryCompiler/Compiler/SystemCompilerCompiledCode.hpp>

#include <Util/SharedLibrary.hpp>

#include <boost/filesystem/operations.hpp>

#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"

#include <Util/Logger.hpp>

#pragma GCC diagnostic pop

namespace NES {

const std::string Compiler::IncludePath = PATH_TO_NES_SOURCE_CODE "/include/";

CompilerPtr Compiler::create() {
    return std::make_shared<Compiler>(Compiler());
}

std::string Compiler::getFileName() {
    auto t = std::time(nullptr);
    auto tm = *std::localtime(&t);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%d-%m-%Y_%H-%M-%S");
    return "gen_query_" + oss.str();
}

CompiledCodePtr Compiler::compile(const std::string& source, bool debugging) {
    if (debugging) {
        formatAndPrintSource(source);
    }
    std::string basename = getFileName();
    std::string filename = basename + ".c";
    std::string libraryName = basename + ".so";
    writeSourceToFile(filename, source);

    auto flags = debugging ? CompilerFlags::createDebuggingCompilerFlags() : CompilerFlags::createDefaultCompilerFlags();
    flags->addFlag("--shared");
    flags->addFlag("-xc++ ");
    flags->addFlag("-I" + IncludePath);
    flags->addFlag("-o" + libraryName);
    flags->addFlag(filename);
    callSystemCompiler(flags);

    auto sharedLibrary = SharedLibrary::load("./" + libraryName);

    return createSystemCompilerCompiledCode(sharedLibrary, basename);
}

void Compiler::callSystemCompiler(CompilerFlagsPtr flags) {
    std::stringstream compilerCall;
    compilerCall << CLANG_EXECUTABLE << " ";

    for (const auto& arg : flags->getFlags()) {
        compilerCall << arg << " ";
    }
    NES_DEBUG("Compiler: compile with: '" << compilerCall.str() << "'");
    auto ret = system(compilerCall.str().c_str());

    if (ret != 0) {
        NES_ERROR("Compiler: compilation failed");
        throw "Compilation failed";
    }
}

void Compiler::formatAndPrintSource(const std::string& source) {
    int ret = system("which clang-format > /dev/null");
    if (ret != 0) {
        NES_ERROR("Compiler: Did not find external tool 'clang-format'. "
                  "Please install 'clang-format' and try again."
                  "If 'clang-format-X' is installed, try to create a "
                  "symbolic link.");
        return;
    }
    const std::string filename = "temporary_file.c";

    writeSourceToFile(filename, source);

    auto formatCommand = std::string("clang-format ") + filename;
    /* try a syntax highlighted output first */
    /* command highlight available? */

    ret = system("which highlight > /dev/null");
    if (ret == 0) {
        formatCommand += " | highlight --src-lang=c++ -O ansi";
    }
    ret = system(formatCommand.c_str());
    std::string cleanupCommand = std::string("rm ") + filename;
    ret = system(cleanupCommand.c_str());
}

void Compiler::writeSourceToFile(const std::string& filename,
                                 const std::string& source) {
    std::ofstream result_file(filename, std::ios::trunc | std::ios::out);
    result_file << source;
}

}// namespace NES
