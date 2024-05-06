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

#include <HDFS/HDFSClient.hpp>

namespace NES {

HDFSClient(const std::string& host, int port) : hdfsHost(host), hdfsPort(port) {
    fs = hdfsConnect(hdfsHost.c_str(), hdfsPort);
    if (!fs) {
        throw std::runtime_error("Failed to connect to HDFS");
    }
}

~HDFSClient() {
    if (fs) {
        hdfsDisconnect(fs);
    }
}

void writeToFile(const std::string& filePath, const std::vector<char>& data) {
    // Open a file for writing
    hdfsFile writeFile = hdfsOpenFile(fs, filePath.c_str(), O_WRONLY | O_CREAT, 0, 0, 0);
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
};