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

#include <Configurations/Persistence/FileStreamCatalogPersistence.hpp>
#include <Util/Logger.hpp>
#include <filesystem>
#include <fstream>
#include <sstream>

namespace NES {

FileStreamCatalogPersistence::FileStreamCatalogPersistence(const std::string& baseDir) : baseDir(baseDir) {
    NES_DEBUG("FileStreamCatalogPersistence: creating FileStreamCatalogPersistence, baseDir=" << baseDir);
    try {
        if (!std::filesystem::exists(baseDir)) {
            NES_DEBUG("FileStreamCatalogPersistence: creating directories at " << baseDir);
            std::filesystem::create_directories(baseDir);
        }
    } catch (std::exception& ex) {
        NES_FATAL_ERROR("FileStreamCatalogPersistence: failed initializing: " << ex.what());
    }
}

bool FileStreamCatalogPersistence::persistLogicalStream(const std::string& logicalStreamName, const std::string& schemaString) {
    std::filesystem::path fileName(logicalStreamName + ".hpp");
    std::filesystem::path filePath = baseDir / fileName;
    NES_DEBUG("FileStreamCatalogPersistence::persistConfiguration: persisting logical stream to "
              << filePath.c_str() << ", logicalStreamName=" << logicalStreamName);

    try {

        std::ofstream ofs(filePath);
        ofs << schemaString;
        ofs << std::endl;
        ofs.close();
        return true;
    } catch (std::exception& ex) {
        NES_ERROR("FileStreamCatalogPersistence::persistConfiguration: persisting failed: " << ex.what());
    }
    return false;
}

bool FileStreamCatalogPersistence::deleteLogicalStream(const std::string& logicalStreamName) {
    std::filesystem::path fileName(logicalStreamName + ".hpp");
    std::filesystem::path filePath = baseDir / fileName;

    std::filesystem::path newFileName(logicalStreamName + ".hpp.deleted");
    std::filesystem::path newFilePath = baseDir / newFileName;

    NES_DEBUG("FileStreamCatalogPersistence::persistConfiguration: persisting logical stream to "
              << filePath.c_str() << ", logicalStreamName=" << logicalStreamName);
    try {
        std::filesystem::rename(filePath, newFilePath);
        return true;
    } catch (std::exception& ex) {
        NES_ERROR("FileStreamCatalogPersistence::persistConfiguration: persisting failed: " << ex.what());
    }
    return false;
}

std::map<std::string, std::string> FileStreamCatalogPersistence::loadLogicalStreams() {
    auto ret = std::map<std::string, std::string>();

    for (const auto& entry : std::filesystem::directory_iterator(baseDir)) {
        if (!entry.is_regular_file() || entry.path().extension() != ".hpp") {
            continue;
        }
        std::string logicalStreamName = entry.path().stem();

        try {
            NES_DEBUG("FileStreamCatalogPersistence::loadLogicalStreams: loading logicalStream \""
                      << logicalStreamName << "\" from " << entry.path().c_str());
            std::ifstream ifs(entry.path());
            std::stringstream ss;
            ifs >> ss.rdbuf();
            ret[logicalStreamName] = ss.str();
        } catch (std::exception& ex) {
            NES_ERROR("FilePhysicalStreamsPersistence::loadConfigurations: failed loading: " << ex.what());
            continue;
        }
    }
    return ret;
}

}// namespace NES
