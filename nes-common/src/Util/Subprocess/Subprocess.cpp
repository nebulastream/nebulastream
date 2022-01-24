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

#include <Util/Logger.hpp>
#include <Util/Subprocess/Subprocess.hpp>
#include <algorithm>
#include <csignal>
#include <cstdio>
#include <fcntl.h>
#include <thread>
#include <vector>

namespace NES::Util {

static const size_t READ_BUFFER_SIZE = 128;

enum ends_of_pipe { READ = 0, WRITE = 1 };

Subprocess::Subprocess(std::string cmd, std::vector<std::string> argv) {
    // initialize pipes
    if (pipe(outPipe) == -1) {
        throw std::system_error(errno, std::system_category());
    }

    argv.insert(argv.begin(), cmd);

    switch (pid = ::fork()) {
        case -1: {
            NES_FATAL_ERROR("Subprocess " << cmd << " failed to start");
            return;
        }
        case 0: {
            executeCommandInChildProcess(argv);
        }
    }
    NES_DEBUG("Started process " << cmd << " with pid: " << pid);

    ::close(outPipe[WRITE]);
    this->outputFile = fdopen(outPipe[READ], "r");
    this->logThread = std::thread([this]() {
        // read till end of process:
        while (!stopped && !feof(outputFile)) {
            readFromFile(outputFile, std::cout);
        }
    });
}

Subprocess::~Subprocess() {
    NES_INFO("Killing process->PID: " << pid);
    ::kill(pid, SIGKILL);
    stopped = true;
    logThread.join();
}

void Subprocess::executeCommandInChildProcess(const std::vector<std::string>& argv) {
    if (dup2(outPipe[WRITE], STDOUT_FILENO) == -1) {
        std::perror("subprocess: dup2() failed");
        return;
    }

    if (outPipe[READ] != -1) {
        ::close(outPipe[READ]);
    }
    ::close(outPipe[WRITE]);

    std::vector<char*> cargs;
    cargs.reserve(argv.size() + 1);
    std::transform(std::begin(argv), std::end(argv), std::back_inserter(cargs), [&](const std::string& str) {
        return (char*) str.c_str();
    });
    cargs.push_back(nullptr);

    if (execvp(cargs[0], &cargs[0]) == -1) {
        std::perror("subprocess: execvp() failed");
        return;
    }
}

void Subprocess::readFromFile(FILE* file, std::ostream& ostream) {
    char buffer[READ_BUFFER_SIZE];
    // use buffer to read and add to result
    if (!feof(file) && fgets((char*) buffer, READ_BUFFER_SIZE, file) != nullptr) {
        ostream << (char*) buffer;
    }
}

}// namespace NES::Util
