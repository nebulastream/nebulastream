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

#ifndef NES_HDFSCLIENT_HPP
#define NES_HDFSCLIENT_HPP

#include "hdfs/hdfs.h"
#include <iostream>
#include <vector>
#include <string>

namespace NES {
    class HDFSClient {
          public:
            HDFSClient(const std::string& hdfsHost, int port);

            void writeToFile(const std::string& fileName, const std::vector<char>& data, uint64_t partitionId);

            hdfsFileInfo* listFiles(const std::string& path, int& numEntries);

            void deleteFile(const std::string& fileName);

            ~HDFSClient();

          private:
            hdfsFS fs;
            std::string hdfsHost;
            int hdfsPort;
        };
}

#endif//NES_HDFSCLIENT_HPP*/