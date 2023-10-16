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
#include <Compiler/ExternalAPI.hpp>
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

using namespace std::string_literals;
namespace NES::Compiler {

const std::string NESCoreIncludePath = PATH_TO_NES_SOURCE_CODE "/nes-core/include/";
const std::string NESCommonIncludePath = PATH_TO_NES_SOURCE_CODE "/nes-common/include/";
const std::string DEBSIncludePath = PATH_TO_DEB_SOURCE_CODE "/include/";

std::shared_ptr<LanguageCompiler> CPPCompiler::create() { return std::make_shared<CPPCompiler>(); }

CPPCompiler::CPPCompiler()
    : format(std::make_unique<ClangFormat>("cpp")), runtimePathConfig(ExecutablePath::loadRuntimePathConfig()) {}

CPPCompiler::~CPPCompiler() noexcept { NES_DEBUG("~CPPCompiler"); }

Language CPPCompiler::getLanguage() const { return Language::CPP; }

CompilationResult CPPCompiler::compile(std::shared_ptr<const CompilationRequest> request) const {
    // Compile and load shared library.
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

    CPPCompilerFlags compilationFlags;
    compilationFlags.addDefaultCompilerFlags();
    compilationFlags.addSharedLibraryFlag();

    if (request->enableOptimizations()) {
        compilationFlags.enableOptimizationFlags();
    }

    if (request->enableDebugging()) {
        compilationFlags.enableDebugFlags();
        format->formatFile(file);
        file->print();
    }

    if (request->enableCompilationProfiling()) {
        compilationFlags.enableProfilingFlags();
    }

    // add header
    for (auto libPaths : runtimePathConfig.libPaths) {
        compilationFlags.addFlag(std::string("-L") + libPaths);
    }

    // add libs
    for (auto libs : runtimePathConfig.libs) {
        compilationFlags.addFlag(libs);
    }

    // add includes
    for (auto includePath : runtimePathConfig.includePaths) {
        compilationFlags.addFlag("-I" + includePath);
    }

    compilationFlags.addFlag("-o" + libraryFileName);

    // the log level of the compiled code is the same as the currently selected log level of the runtime.
    auto logLevel = getLogLevel(Logger::getInstance()->getCurrentLogLevel());
    compilationFlags.addFlag("-DFMT_HEADER_ONLY"s);
    compilationFlags.addFlag("-DNES_COMPILE_TIME_LOG_LEVEL=" + std::to_string(logLevel));

    for (auto api : request->getExternalAPIs()) {
        compilationFlags.mergeFlags(api->getCompilerFlags());
    }

    // lock file, such that no one can operate on the file at the same time
    const std::lock_guard<std::mutex> fileLock(file->getFileMutex());

    std::stringstream compilerCall;
    compilerCall << runtimePathConfig.clangBinaryPath << " ";
    for (const auto& arg : compilationFlags.getFlags()) {
        compilerCall << arg << " ";
    }

    compilerCall << file->getPath();

    NES_DEBUG("Compiler: compile with: '{}'", compilerCall.str());
    // Creating a pointer to an open stream and a buffer, to read the output of the compiler
    FILE* fp = nullptr;
    char buffer[8192];

    // Redirecting stderr to stdout, to be able to read error messages
    compilerCall << " 2>&1";

    // Calling the compiler in a new process
    fp = popen(compilerCall.str().c_str(), "r");

    if (fp == nullptr) {
        NES_ERROR("Compiler: failed to run command\n");
        throw std::runtime_error("Compiler: failed to run command");
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
        NES_ERROR("Compiler: compilation of {} failed.", libraryFileName);
        throw std::runtime_error(strstream.str());
    }

    if (!request->enableDebugging()) {
        std::filesystem::remove(sourceFileName);
    }
    auto sharedLibrary = SharedLibrary::load(libraryFileName);

    std::filesystem::remove(libraryFileName);

    timer.pause();
    NES_INFO("[CPPCompiler] Compilation time: {}ms", (double) timer.getRuntime() / (double) 1000000);

    return CompilationResult(sharedLibrary, std::move(timer));
}

}// namespace NES::Compiler