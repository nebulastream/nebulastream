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
#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"

#include <Util/Logger.hpp>
#include <cstring>
#include <filesystem>

#pragma GCC diagnostic pop

namespace NES {

const std::string Compiler::IncludePath = PATH_TO_NES_SOURCE_CODE "/include/";

CompilerPtr Compiler::create() { return std::make_shared<Compiler>(); }

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
//    flags->addFlag("-xc++ ");
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
#if 0
// TODO the coded below does not work as it cannot find ld
    pid_t compiler_pid;
    int status;
    int pipe_fd[2];
    char** argv = new char*[flags->getFlags().size() + 2];

    argv[0] = "clang++"; // new char[strlen(CLANG_EXECUTABLE)];
//    strncpy(argv[0], CLANG_EXECUTABLE, strlen(CLANG_EXECUTABLE));
//    argv[0][strlen(CLANG_EXECUTABLE)] = 0;
    int argc = 1;
    for (const auto& arg : flags->getFlags()) {
        compilerCall << arg << " ";
        argv[argc] = new char[arg.size() + 1];
        strncpy(argv[argc], arg.c_str(), arg.size());
        argv[argc][arg.size()] = '\0';
        NES_DEBUG("Arg " << argv[argc]);
        argc++;
    }
    argv[argc] = NULL;

    NES_ASSERT(pipe(pipe_fd) != -1, "Cannot create pipe");

    NES_DEBUG("Compiler: compile with: '" << compilerCall.str() << "'");

    char *envp[] = { NULL, NULL };
    envp[0] = getenv("PATH");
    if (0 == (compiler_pid = fork())) {
        dup2(pipe_fd[1], STDOUT_FILENO);
        close(pipe_fd[0]);
        close(pipe_fd[1]);
        for (int i = 0; i < argc; i++) {
            fprintf(stderr, "%d=%s\n", i, argv[i]);
        }
        fprintf(stderr, "%s\n", envp[0]);
        if (-1 == execve(CLANG_EXECUTABLE, argv, envp)) {
            NES_ERROR("Compiler: compilation failed: " << strerror(errno));
            for (int i = 0; i < argc; i++) {
               delete argv[i];
            }
            delete[] argv;
            throw "Compilation failed";
        }
    }

    char temp[4096];
    close(pipe_fd[1]);
    size_t nbytes = 0;
    size_t totalBytes = 0;
    std::stringstream output;
    while (0 != (nbytes = read(pipe_fd[0], temp, sizeof(temp)))) {
        output << temp;
        memset(temp, 0, sizeof(temp));
        totalBytes += nbytes;
    }

    while (0 == waitpid(compiler_pid, &status, WNOHANG)) {
        usleep(100'000);// 100ms
    }

    if (1 != WIFEXITED(status) || 0 != WEXITSTATUS(status)) {
        NES_ERROR("Compiler: compilation failed: " << strerror(errno));
        for (int i = 0; i < argc; i++) {
            delete argv[i];
        }
        delete[] argv;
        throw "Compilation failed";
    }

    for (int i = 0; i < argc; i++) {
        delete argv[i];
    }

    delete[] argv;

    NES_INFO("Compiler output(" << totalBytes << "):\n " << output.str());

#else
    for (const auto& arg : flags->getFlags()) {
        compilerCall << arg << " ";
    }
    auto ret = system(compilerCall.str().c_str());
    if (ret != 0) {
        NES_ERROR("Compiler: compilation failed");
        throw "Compilation failed";
    }
#endif

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

    auto formatCommand = "clang-format -i " + filename;

    auto res = popen(formatCommand.c_str(), "r");
    if (!res) {
        NES_FATAL_ERROR("Compiler: popen() failed!");
    }
    // wait till command is complete executed.
    pclose(res);
    // read source file in
    std::ifstream file(filename);
    file.clear();
    std::string sourceCode((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    NES_DEBUG("Compiler: generate code: \n" << sourceCode);
    return sourceCode;
}

void Compiler::writeSourceToFile(const std::string& filename, const std::string& source) {
    auto path = std::filesystem::current_path().string();
    NES_DEBUG("Compiler: write source to file://" << path << "/" << filename);
    std::ofstream result_file(filename, std::ios::trunc | std::ios::out);
    result_file << source;
}

}// namespace NES
