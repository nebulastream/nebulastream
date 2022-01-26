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

#ifndef NES_INCLUDE_UTIL_SUBPROCESS_SUBPROCESS_HPP_
#define NES_INCLUDE_UTIL_SUBPROCESS_SUBPROCESS_HPP_
#include <atomic>
#include <cstdio>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <vector>

namespace NES::Util {

/**
 * @brief This class spawns a new subprocess and executes a specific command.
 * As soon as the subprocess object goes out of scope the subprocess is terminated.
 */
class Subprocess {
  public:
    /**
     * @brief Spawns a new subprocess with a specific cmd command and arguments.
     * @param cmd that should be executed
     * @param argv arguments
     */
    Subprocess(std::string cmd, std::vector<std::string> argv);
    ~Subprocess();

  private:
    /**
     * @brief read a _IO_FILE and pipe the content to an output stream
     * @param file _IO_FILE
     * @param ostream
     */
    static void readFromFile(FILE* file, std::ostream& ostream);

    /**
     * @brief execute the cmd and all its arguments in the child process.
     * @param argv arguments
     */
    void executeCommandInChildProcess(const std::vector<std::string>& argv);
    pid_t pid;
    int outPipe[2];
    FILE* outputFile;
    std::atomic_bool stopped = false;
    std::thread logThread;
};

}// namespace NES::Util

#endif  // NES_INCLUDE_UTIL_SUBPROCESS_SUBPROCESS_HPP_
