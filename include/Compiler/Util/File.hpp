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
#ifndef NES_INCLUDE_COMPILER_FILE_HPP_
#define NES_INCLUDE_COMPILER_FILE_HPP_
#include <memory>
#include <mutex>
#include <string>
namespace NES::Compiler {

/**
 * @brief Represents for a file at a specific path
 */
class File {
  public:
    /**
     * @brief Constructor for a file at a specific path
     * @param path
     */
    explicit File(std::string path);

    /**
     * @brief Creates a new file form a string.
     * @param path target path of the file.
     * @param content of the file
     * @return std::shared_ptr<File>
     */
    static std::shared_ptr<File> createFile(const std::string& path, const std::string& content);

    /**
     * @brief Returns the path of the file
     * @return path
     */
    std::string getPath() const;

    /**
     * @brief Reads the file as a string
     * @return file content
     */
    std::string read() const;

    /**
     * @brief Prints the file to the log
     */
    void print() const;

    /**
     * @brief Gives access to a mutex to lock the file
     * @return std::mutex
     */
    std::mutex& getFileMutex();

  private:
    const std::string path;
    mutable std::mutex fileMutex;
};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_FILE_HPP_
