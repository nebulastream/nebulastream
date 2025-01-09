/*/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
#1#

#include <HDFS/HDFSClient.hpp>
#include "hdfs/hdfs.h"

namespace NES {

HDFSClient::HDFSClient(const std::string& host, int port) : hdfsHost(host), hdfsPort(port) {
    fs = hdfsConnect(hdfsHost.c_str(), hdfsPort);
    if (!fs) {
        throw std::runtime_error("Failed to connect to HDFS");
    }
}

HDFSClient::~HDFSClient() {
    if (fs) {
        hdfsDisconnect(fs);
    }
}

void HDFSClient::writeToFile(const std::string& filePath, const std::vector<char>& data, uint64_t partitionId) {
    // Open a file for writing
    std::string dirPath = "/user/hadoop/" + std::to_string(partitionId);
    int createDir = hdfsCreateDirectory(fs, dirPath.c_str());
    if(createDir == -1) {
        throw std::runtime_error("Failed to create directory for partition " + std::to_string(partitionId));
    }

    hdfsFile writeFile = hdfsOpenFile(fs, filePath.c_str(), O_WRONLY | O_APPEND, 0, 0, 0);
    if (!writeFile) {
        throw std::runtime_error("Failed to open file for writing");
    }

    // Write data to the file
    tSize written = hdfsWrite(fs, writeFile, data.data(), data.size());
    if (written == -1) {
        hdfsCloseFile(fs, writeFile);
        throw std::runtime_error("Failed to write to file");
    }

    // Flush and close the file
    if (hdfsFlush(fs, writeFile)) {
        hdfsCloseFile(fs, writeFile);
        throw std::runtime_error("Failed to flush file");
    }

    hdfsCloseFile(fs, writeFile);
}

hdfsFileInfo* HDFSClient::listFiles(const std::string& path, int& numEntries) {
    hdfsFileInfo* fileList = hdfsListDirectory(fs, path.c_str(), &numEntries);
    if(!fileList) {
        throw std::runtime_error("Failed to list directory");
    }
    return fileList;
}

void HDFSClient::deleteFile(const std::string& filePath) {
    int deleted = hdfsDelete(fs, filePath.c_str(), 0);
    if (deleted == -1) {
        throw std::runtime_error("Failed to delete file: " + filePath);
    }
}
};*/