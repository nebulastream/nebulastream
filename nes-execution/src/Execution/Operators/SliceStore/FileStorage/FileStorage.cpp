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

#include <Execution/Operators/SliceStore/FileStorage/FileStorage.hpp>
#include <sstream>

// Writer Implementation
FileWriter::FileWriter(const std::string& file_path) {
    file_.open(file_path, std::ios::out | std::ios::app);
    if (!file_.is_open()) {
        throw std::ios_base::failure("Failed to open file for writing");
    }
}

FileWriter::~FileWriter() {
    if (file_.is_open()) {
        file_.close();
    }
}

template<typename T>
void FileWriter::write(const T& data) {
    if (file_.is_open()) {
        file_ << data << "\n";
    }
}

// Reader Implementation
FileReader::FileReader(const std::string& file_path) {
    file_.open(file_path, std::ios::in);
    if (!file_.is_open()) {
        throw std::ios_base::failure("Failed to open file for reading");
    }
}

FileReader::~FileReader() {
    if (file_.is_open()) {
        file_.close();
    }
}

template<typename T>
std::vector<T> FileReader::read() {
    std::vector<T> data_list;
    std::string line;

    while (std::getline(file_, line)) {
        std::istringstream iss(line);
        T data;
        iss >> data;
        data_list.push_back(data);
    }

    return data_list;
}
