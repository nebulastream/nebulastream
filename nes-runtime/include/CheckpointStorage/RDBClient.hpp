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

#ifndef NES_RDBCLIENT_HPP
#define NES_RDBCLIENT_HPP

#include "hdfs/hdfs.h"
#include <rocksdb/db.h>
#include <string>
#include <memory>

namespace NES {
    class RDBClient {
    public:
        RDBClient(std::string& dbPath, const std::string& hdfsHost, int port);

        ~RDBClient();

        void writeEntry(std::string& key, std::string& value, uint64_t partitionId);

        hdfsFileInfo* listFiles(const std::string& path, int& numEntries);

        void deleteEntry(const std::string& fileName);

    private:
        rocksdb::DB* db;
        hdfsFS fs;
        std::string hdfsHost;
        int hdfsPort;
    };
}

#endif//NES_RDBCLIENT_HPP