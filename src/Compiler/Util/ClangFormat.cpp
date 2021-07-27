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
#include <Compiler/Exceptions/CompilerException.hpp>
#include <Compiler/Util/ClangFormat.hpp>
#include <Util/Logger.hpp>
namespace NES::Compiler {

ClangFormat::ClangFormat(const std::string language) : language(language) {}

void ClangFormat::formatFile(std::shared_ptr<File> file) {
    // lock clang format
    const std::lock_guard<std::mutex> lock(clangFormatMutex);
    // lock file, such that no one can operate on the file at the same time
    const std::lock_guard<std::mutex> fileLock(file->getFileMutex());
    // check if we can access clang-format
    // TODO replace with build-in clang-format #2074
    int ret = system("which clang-format > /dev/null");
    if (ret != 0) {
        throw CompilerException("Compiler: Did not find external tool 'clang-format'. "
                                "Please install 'clang-format' and try again."
                                "If 'clang-format-X' is installed, try to create a symbolic link.");
    }
    // construct clang-format command argument
    auto formatCommand = "clang-format --assume-filename=" + language + " -i " + file->getPath();
    NES_DEBUG("Format with " << formatCommand);
    auto* res = popen(formatCommand.c_str(), "r");
    if (res == nullptr) {
        throw CompilerException("ClangFormat: popen() failed!");
    }
    // wait till command is complete executed.
    pclose(res);
}

}// namespace NES::Compiler