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

#include <Util/Logger/Logger.hpp>
#include <Util/Subprocess/Subprocess.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <algorithm>
#include <csignal>
#include <cstdio>
#include <thread>
#include <vector>

namespace NES::Util {

static const size_t READ_BUFFER_SIZE = 128;

enum class ends_of_pipe : uint8_t { READ = 0, WRITE = 1 };

Subprocess::Subprocess(std::string cmd, std::vector<std::string> argv) {
    // initialize pipes
    if (pipe(outPipe) == -1) {
        throw std::system_error(errno, std::system_category());
    }

    std::cout << "Going to execute: " << cmd << " ";
    for (auto& p : argv) {
        std::cout << p << " ";
    }
    std::cout << std::endl;

    argv.insert(argv.begin(), cmd);

    switch (pid = ::fork()) {
        case -1: {
            NES_FATAL_ERROR("Subprocess {} failed to start", cmd);
            return;
        }
        case 0: {
            executeCommandInChildProcess(argv);
        }
    }
    NES_DEBUG("Started process {} with pid: {}", cmd, pid);

    ::close(outPipe[magic_enum::enum_integer(ends_of_pipe::WRITE)]);
    this->outputFile = fdopen(outPipe[magic_enum::enum_integer(ends_of_pipe::READ)], "r");
    this->logThread = std::thread([this]() {
        // read till end of process:
        while (!stopped && !feof(outputFile)) {
            readFromFile(outputFile, std::cout);
        }
    });
}

Subprocess::~Subprocess() {
    NES_INFO("Killing process->PID: {}", pid);
    ::kill(pid, SIGKILL);
    stopped = true;
    logThread.join();
}

bool Subprocess::kill() {
    NES_DEBUG("Killing pid={}", pid);
    auto res = ::kill(pid, SIGKILL);
    NES_ASSERT(res == 0, "process could not be killed");
    NES_DEBUG("Process pid={} successfully killed", pid);
    return res;
}

uint64_t Subprocess::getPid() { return pid; };

void Subprocess::executeCommandInChildProcess(const std::vector<std::string>& argv) {
    if (dup2(outPipe[magic_enum::enum_integer(ends_of_pipe::WRITE)], STDOUT_FILENO) == -1) {
        std::perror("subprocess: dup2() failed");
        return;
    }

    if (outPipe[magic_enum::enum_integer(ends_of_pipe::READ)] != -1) {
        ::close(outPipe[magic_enum::enum_integer(ends_of_pipe::READ)]);
    }
    ::close(outPipe[magic_enum::enum_integer(ends_of_pipe::WRITE)]);

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
