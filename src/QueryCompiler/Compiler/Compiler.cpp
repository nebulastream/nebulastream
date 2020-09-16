#include <QueryCompiler/Compiler/CompiledCode.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <QueryCompiler/Compiler/CompilerFlags.hpp>
#include <QueryCompiler/Compiler/SystemCompilerCompiledCode.hpp>

#include <Util/SharedLibrary.hpp>

#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"

#include <Util/Logger.hpp>
#include <filesystem>

#pragma GCC diagnostic pop

namespace NES {

const std::string Compiler::IncludePath = PATH_TO_NES_SOURCE_CODE "/include/";

CompilerPtr Compiler::create() {
    return std::make_shared<Compiler>();
}

std::string Compiler::getFileName() {
    auto time = std::time(nullptr);
    auto localtime = *std::localtime(&time);

    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist(1, 10000);

    std::ostringstream oss;
    oss << "gen_query_" << std::put_time(&localtime, "%d-%m-%Y_%H-%M-%S") << "_" << dist(rng);
    return oss.str();
}

CompiledCodePtr Compiler::compile(const std::string& source, bool debugging) {

    // write source to cpp file
    std::string basename = getFileName();
    std::string filename = basename + ".cpp";
    std::string libraryName = basename + ".so";
    writeSourceToFile(filename, source);

    // if we are in compile in debugging mode we create print the source file.
    if (debugging) {
        formatAndPrintSource(filename);
    }

    // init compilation flag dependent on compilation mode
    auto flags = debugging ? CompilerFlags::createDebuggingCompilerFlags() : CompilerFlags::createDefaultCompilerFlags();
    flags->addFlag("--shared");
    flags->addFlag("-xc++ ");
    flags->addFlag("-I" + IncludePath);
    flags->addFlag("-o" + libraryName);
    flags->addFlag(filename);

    // call compiler to generate shared lib from source code
    callSystemCompiler(flags);
    // load shared lib
    auto sharedLibrary = SharedLibrary::load("./" + libraryName);
    return SystemCompilerCompiledCode::create(sharedLibrary, basename);
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

std::string Compiler::formatAndPrintSource(const std::string& filename) {
    int ret = system("which clang-format > /dev/null");
    if (ret != 0) {
        NES_ERROR("Compiler: Did not find external tool 'clang-format'. "
                  "Please install 'clang-format' and try again."
                  "If 'clang-format-X' is installed, try to create a "
                  "symbolic link.");
        return "";
    }

    auto formatCommand = std::string("clang-format ") + filename;
    std::array<char, 128> buffer;
    std::string result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(formatCommand.c_str(), "r"), pclose);
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
        result += buffer.data();
    }
    NES_DEBUG("Compiler: generate code: \n" << result);
    return result;
}

void Compiler::writeSourceToFile(const std::string& filename,
                                 const std::string& source) {
    auto path = std::filesystem::current_path().string();
    NES_DEBUG("Compiler: write source to " << path <<"/"<< filename);
    std::ofstream result_file(filename, std::ios::trunc | std::ios::out);
    result_file << source;
}

}// namespace NES
