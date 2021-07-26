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
#include <Compiler/Util/SharedLibrary.hpp>
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/Util/File.hpp>
#include <Compiler/SourceCode.hpp>
#include <Compiler/Util/ClangFormat.hpp>
#include <Util/Logger.hpp>
#include <sstream>
#include <iostream>
#include <chrono>

namespace NES::Compiler {

const std::string NESIncludePath = PATH_TO_NES_SOURCE_CODE "/include/";
const std::string DEBSIncludePath = PATH_TO_DEB_SOURCE_CODE "/include/";

std::shared_ptr<LanguageCompiler> CPPCompiler::create() { return std::make_shared<CPPCompiler>(); }

CPPCompiler::CPPCompiler() : format(std::make_unique<ClangFormat>("cpp")) {}

std::string CPPCompiler::getLanguage() const { return "cpp"; }

CompilationResult CPPCompiler::compile(std::unique_ptr<const CompilationRequest> request) const {
    auto start = std::chrono::steady_clock::now();
    auto sourceFileName = request->getName() + ".cpp";
    auto libraryFileName = request->getName() + ".so";

    auto file = File::createFile(sourceFileName, request->getSourceCode()->getCode());


    auto compilationFlags = CPPCompilerFlags::createDefaultCompilerFlags();
    if (request->enableOptimizations()) {
        compilationFlags.enableOptimizationFlags();
    }
    if (request->enableDebugging()) {
        compilationFlags.enableDebugFlags();
        format->formatFile(file);
        file->print();
    }

    compilationFlags.addFlag("--shared");
    // add header of NES Source
    compilationFlags.addFlag("-I" + NESIncludePath);
    // add header of all dependencies
    compilationFlags.addFlag("-I" + DEBSIncludePath);

    compilationFlags.addFlag("-o" + libraryFileName);

#ifdef NES_LOGGING_TRACE_LEVEL
    compilationFlags.addFlag("-DNES_LOGGING_TRACE_LEVEL=1");
#endif
#ifdef NES_LOGGING_DEBUG_LEVEL
    flags->addFlag("-DNES_LOGGING_DEBUG_LEVEL=1");
#endif
#ifdef NES_LOGGING_INFO_LEVEL
    flags->addFlag("-DNES_LOGGING_INFO_LEVEL=1");
#endif
#ifdef NES_LOGGING_WARNING_LEVEL
    flags->addFlag("-DNES_LOGGING_WARNING_LEVEL=1");
#endif
#ifdef NES_LOGGING_ERROR_LEVEL
    flags->addFlag("-DNES_LOGGING_ERROR_LEVEL=1");
#endif
#ifdef NES_LOGGING_NO_LEVEL
    flags->addFlag("-DNES_LOGGING_NO_LEVEL=1");
#endif
#ifdef NES_LOGGING_FATAL_ERROR_LEVEL
    flags->addFlag("-DNES_LOGGING_FATAL_ERROR_LEVEL=1");
#endif

    compilationFlags.addFlag(sourceFileName);


    compileSharedLib(compilationFlags, file, libraryFileName);
    // load shared lib
    auto sharedLibrary = SharedLibrary::load(libraryFileName);
    auto end = std::chrono::steady_clock::now();
    // TODO replace by timer util with issue #2062
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    return CompilationResult(sharedLibrary, duration);
}

void CPPCompiler::compileSharedLib(CPPCompilerFlags flags, std::shared_ptr<File> sourceFile, std::string libraryFileName) const{
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