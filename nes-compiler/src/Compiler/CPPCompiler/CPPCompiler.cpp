/*
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
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/CPPCompiler/CPPCompilerFlags.hpp>
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/SourceCode.hpp>
#include <Compiler/Util/ClangFormat.hpp>
#include <Compiler/Util/ExecutablePath.hpp>
#include <Compiler/Util/File.hpp>
#include <Compiler/Util/SharedLibrary.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <unistd.h>
namespace NES::Compiler {

const std::string NESCoreIncludePath = PATH_TO_NES_SOURCE_CODE "/nes-core/include/";
const std::string NESCommonIncludePath = PATH_TO_NES_SOURCE_CODE "/nes-common/include/";
const std::string DEBSIncludePath = PATH_TO_DEB_SOURCE_CODE "/include/";

std::shared_ptr<LanguageCompiler> CPPCompiler::create() { return std::make_shared<CPPCompiler>(); }

CPPCompiler::CPPCompiler()
    : format(std::make_unique<ClangFormat>("cpp")), runtimePathConfig(ExecutablePath::loadRuntimePathConfig()) {}

CPPCompiler::~CPPCompiler() noexcept { NES_DEBUG("~CPPCompiler"); }

std::string CPPCompiler::getLanguage() const { return "cpp"; }

CompilationResult CPPCompiler::compile(std::shared_ptr<const CompilationRequest> request) const {
    Timer timer("CPPCompiler");
    timer.start();
    std::string fileName = (std::filesystem::temp_directory_path() / request->getName());
    auto sourceFileName = fileName + ".cpp";
    auto libraryFileName = fileName +
#ifdef __linux__
        ".so";
#elif defined(__APPLE__)
        ".dylib";
#else
#error "Unknown platform"
#endif
    auto& sourceCode = request->getSourceCode()->getCode();
    NES_ASSERT2_FMT(sourceCode.size(), "empty source code for " << sourceFileName);
    auto file = File::createFile(sourceFileName, sourceCode);
    auto compilationFlags = CPPCompilerFlags::createDefaultCompilerFlags();
    if (request->enableOptimizations()) {
        NES_DEBUG("Compile with optimizations.");
        compilationFlags.enableOptimizationFlags();
    }
    if (request->enableDebugging()) {
        NES_DEBUG("Compile with debugging.");
        compilationFlags.enableDebugFlags();
        format->formatFile(file);
        file->print();
    }
    if (request->enableCompilationProfiling()) {
        compilationFlags.addFlag(CPPCompilerFlags::TRACE_COMPILATION_TIME);
        NES_DEBUG("Compilation Time tracing is activated open: chrome://tracing/");
    }
    compilationFlags.addFlag("-shared");
    compilationFlags.addFlag("-g");

    // add header
    for (auto libPaths : runtimePathConfig.libPaths) {
        compilationFlags.addFlag(std::string("-L") + libPaths);
    }
    // add libs
    for (auto libs : runtimePathConfig.libs) {
        compilationFlags.addFlag(libs);
    }
    // add header
    for (auto includePath : runtimePathConfig.includePaths) {
        compilationFlags.addFlag("-I" + includePath);
    }

    compilationFlags.addFlag("-o" + libraryFileName);

    // the log level of the compiled code is the same as the currently selected log level of the runtime.
    auto logLevel = getLogLevel(Logger::getInstance().getCurrentLogLevel());
    compilationFlags.addFlag("-DFMT_HEADER_ONLY -DNES_COMPILE_TIME_LOG_LEVEL=" + std::to_string(logLevel));

#ifdef TFDEF
    compilationFlags.addFlag("-DTFDEF=1");
#endif// TFDEF

    compilationFlags.addFlag(sourceFileName);

    compileSharedLib(compilationFlags, file, libraryFileName);

    // load shared lib
    auto sharedLibrary = SharedLibrary::load(libraryFileName);

    timer.pause();
    if (!request->enableDebugging()) {
        std::filesystem::remove(sourceFileName);
        std::filesystem::remove(libraryFileName);
    }
    NES_INFO("CPPCompiler Runtime: " << (double) timer.getRuntime() / (double) 1000000 << "ms");// print runtime
    std::filesystem::remove(libraryFileName);
    return CompilationResult(sharedLibrary, std::move(timer));
}

void CPPCompiler::compileSharedLib(CPPCompilerFlags flags, std::shared_ptr<File> sourceFile, std::string libraryFileName) const {
    // lock file, such that no one can operate on the file at the same time
    const std::lock_guard<std::mutex> fileLock(sourceFile->getFileMutex());

    std::stringstream compilerCall;
    compilerCall << runtimePathConfig.clangBinaryPath << " ";
    for (const auto& arg : flags.getFlags()) {
        compilerCall << arg << " ";
    }
    NES_DEBUG("Compiler: compile with: '" << compilerCall.str() << "'");
    NES_DEBUG("Compiler: compile with: '" << compilerCall.str() << "'");
    // Creating a pointer to an open stream and a buffer, to read the output of the compiler
    FILE* fp = nullptr;
    char buffer[8192];

    // Redirecting stderr to stdout, to be able to read error messages
    compilerCall << " 2>&1";

    // Calling the compiler in a new process
    fp = popen(compilerCall.str().c_str(), "r");

    if (fp == nullptr) {
        NES_ERROR("Compiler: failed to run command\n");
        return;
    }

    // Collecting the output of the compiler to a string stream
    std::ostringstream strstream;
    while (fgets(buffer, sizeof(buffer), fp) != nullptr) {
        strstream << buffer;
    }

    // Closing the stream, which also gives us the exit status of the compiler call
    auto ret = pclose(fp);

    // If the compilation didn't return with 0, we throw an exception containing the compiler output
    if (ret != 0) {
        NES_ERROR("Compiler: compilation of " << libraryFileName << " failed.");
        throw std::runtime_error(strstream.str());
    }
}

}// namespace NES::Compiler