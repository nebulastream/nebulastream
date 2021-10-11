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
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/CPPCompiler/CPPCompilerFlags.hpp>
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/SourceCode.hpp>
#include <Compiler/Util/ClangFormat.hpp>
#include <Compiler/Util/File.hpp>
#include <Compiler/Util/SharedLibrary.hpp>
#include <Util/Logger.hpp>
#include <Util/Timer.hpp>
#include <chrono>
#include <iostream>
#include <sstream>
#include <string>
namespace NES::Compiler {

const std::string NESIncludePath = PATH_TO_NES_SOURCE_CODE "/include/";
const std::string DEBSIncludePath = PATH_TO_DEB_SOURCE_CODE "/include/";

#ifdef __APPLE__
#include <mach-o/dyld.h>

namespace detail {
std::filesystem::path getExecutablePath() {
    typedef std::vector<char> char_vector;
    char_vector buf(1024, 0);
    uint32_t size = static_cast<uint32_t>(buf.size());
    bool havePath = false;
    bool shouldContinue = true;
    do {
        int result = _NSGetExecutablePath(&buf[0], &size);
        if (result == -1) {
            buf.resize(size + 1);
            std::fill(std::begin(buf), std::end(buf), 0);
        } else {
            shouldContinue = false;
            if (buf.at(0) != 0) {
                havePath = true;
            }
        }
    } while (shouldContinue);
    if (!havePath) {
        return std::filesystem::current_path();
    }
    std::error_code ec;
    std::string path(&buf[0], size);
    std::filesystem::path p(std::filesystem::canonical(path, ec));
    if (!ec) {
        return p.make_preferred();
    }
    return std::filesystem::current_path();
}
std::filesystem::path recursiveFindFileReverse(std::filesystem::path currentPath, const std::string targetFileName) {
    while (!std::filesystem::is_directory(currentPath)) {
        currentPath = currentPath.parent_path();
    }
    while (currentPath != currentPath.root_directory()) {
        for (const auto& entry : std::filesystem::directory_iterator(currentPath)) {
            if (entry.is_directory()) {
                continue;
            }
            auto path = entry.path();
            auto fname = path.filename();
            if (fname.string().compare(targetFileName) == 0) {
                return path;
            }
        }
        currentPath = currentPath.parent_path();
    }
    return currentPath;
}

}// namespace detail
#endif

std::shared_ptr<LanguageCompiler> CPPCompiler::create() { return std::make_shared<CPPCompiler>(); }

CPPCompiler::CPPCompiler() : format(std::make_unique<ClangFormat>("cpp")) {
#ifdef __APPLE__
    libNesPath = detail::recursiveFindFileReverse(detail::getExecutablePath(), "libnes.dylib");
    NES_ASSERT2_FMT(std::filesystem::is_regular_file(libNesPath), "Invalid libnes.dylib file found at " << libNesPath);
    NES_DEBUG("Library libnes.dylib found at: " << libNesPath.parent_path());
#endif
}

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
#ifdef __linux__
    compilationFlags.addFlag("--shared");
#elif defined(__APPLE__)
    compilationFlags.addFlag("-shared");
    compilationFlags.addFlag("-lnes");
    compilationFlags.addFlag(std::string("-L") + libNesPath.parent_path().string());
#else
#error "Unknown platform"
#endif
    // add header of NES Source
    compilationFlags.addFlag("-I" + NESIncludePath);
    // add header of all dependencies
    compilationFlags.addFlag("-I" + DEBSIncludePath);

    compilationFlags.addFlag("-o" + libraryFileName);

#ifdef NES_LOGGING_TRACE_LEVEL
    compilationFlags.addFlag("-DNES_LOGGING_TRACE_LEVEL=1");
#endif
#ifdef NES_LOGGING_DEBUG_LEVEL
    compilationFlags.addFlag("-DNES_LOGGING_DEBUG_LEVEL=1");
#endif
#ifdef NES_LOGGING_INFO_LEVEL
    compilationFlags.addFlag("-DNES_LOGGING_INFO_LEVEL=1");
#endif
#ifdef NES_LOGGING_WARNING_LEVEL
    compilationFlags.addFlag("-DNES_LOGGING_WARNING_LEVEL=1");
#endif
#ifdef NES_LOGGING_ERROR_LEVEL
    compilationFlags.addFlag("-DNES_LOGGING_ERROR_LEVEL=1");
#endif
#ifdef NES_LOGGING_NO_LEVEL
    compilationFlags.addFlag("-DNES_LOGGING_NO_LEVEL=1");
#endif
#ifdef NES_LOGGING_FATAL_ERROR_LEVEL
    compilationFlags.addFlag("-DNES_LOGGING_FATAL_ERROR_LEVEL=1");
#endif

    compilationFlags.addFlag(sourceFileName);

    compileSharedLib(compilationFlags, file, libraryFileName);

    // load shared lib
    auto sharedLibrary = SharedLibrary::load(libraryFileName);

    timer.pause();
    if (!request->enableDebugging()) {
        std::filesystem::remove(sourceFileName);
    }
    NES_INFO("CPPCompiler Runtime: " << (double) timer.getRuntime() / (double) 1000000 << "ms");// print runtime

    return CompilationResult(sharedLibrary, std::move(timer));
}

void CPPCompiler::compileSharedLib(CPPCompilerFlags flags, std::shared_ptr<File> sourceFile, std::string libraryFileName) const {
    // lock file, such that no one can operate on the file at the same time
    const std::lock_guard<std::mutex> fileLock(sourceFile->getFileMutex());

    std::stringstream compilerCall;
    compilerCall << CLANG_EXECUTABLE << " ";
    for (const auto& arg : flags.getFlags()) {
        compilerCall << arg << " ";
    }
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

    // If the compilation did't return with 0, we throw an exception containing the compiler output
    if (ret != 0) {
        NES_ERROR("Compiler: compilation of " << libraryFileName << " failed.");
        throw std::runtime_error(strstream.str());
    }
}

}// namespace NES::Compiler