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

#include <CheckpointStorage/RDBClient.hpp>
#include "hdfs/hdfs.h"
#include <Util/Logger/Logger.hpp>

namespace NES {

    RDBClient::RDBClient(std::string& dbPath, const std::string& host, int port) : hdfsHost(host), hdfsPort(port) {
        // RocksDB
        rocksdb::Options rocksdbOptions;
        rocksdbOptions.create_if_missing = true;
        rocksdb::Status status = rocksdb::DB::Open(rocksdbOptions, dbPath, &db);
        if (!status.ok()) {
            NES_ERROR("Failed to open RocksDB: %s", status.ToString().c_str());
        }

        // HDFS
        fs = hdfsConnect(hdfsHost.c_str(), hdfsPort);
        if (!fs) {
            throw std::runtime_error("Failed to connect to HDFS");
        }
    }

    RDBClient::~RDBClient() {
        delete db;
        if (fs) {
            hdfsDisconnect(fs);
        }
    }

    void RDBClient::writeEntry(std::string& key, std::string& value, uint64_t partitionId) {
        rocksdb::Status status = db->Put(rocksdb::WriteOptions(), key, value);
        if (!status.ok()) {
            NES_ERROR("Failed to write into RocksDB: %s", status.ToString().c_str());
        }

        // Open a file for writing
        std::string dirPath = "/user/hadoop/" + std::to_string(partitionId);
        int createDir = hdfsCreateDirectory(fs, dirPath.c_str());
        if(createDir == -1) {
            throw std::runtime_error("Failed to create directory for partition " + std::to_string(partitionId));
        }

        hdfsFile writeFile = hdfsOpenFile(fs, key.c_str(), O_WRONLY | O_APPEND, 0, 0, 0);
        if (!writeFile) {
            throw std::runtime_error("Failed to open file for writing");
        }

        // Write data to the file
        tSize written = hdfsWrite(fs, writeFile, value.data(), value.size());
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

    hdfsFileInfo* RDBClient::listFiles(const std::string& path, int& numEntries) {
        hdfsFileInfo* fileList = hdfsListDirectory(fs, path.c_str(), &numEntries);
        if(!fileList) {
            throw std::runtime_error("Failed to list directory");
        }
        return fileList;
    }

    void RDBClient::deleteEntry(const std::string& key) {
        int deleted = hdfsDelete(fs, key.c_str(), 0);
        if (deleted == -1) {
            throw std::runtime_error("Failed to delete file: " + key);
        }
    }
};