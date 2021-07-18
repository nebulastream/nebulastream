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

#include <Configurations/ConfigOption.hpp>
#include <Persistence/FilePhysicalStreamsPersistence.hpp>
#include <Util/Logger.hpp>
#include <filesystem>
#include <fstream>

namespace NES {

FilePhysicalStreamsPersistence::FilePhysicalStreamsPersistence(const std::string& baseDir) : baseDir(baseDir) {
    NES_DEBUG("FilePhysicalStreamsPersistence: creating FilePhysicalStreamsPersistence, baseDir=" << baseDir);
    if (!std::filesystem::exists(baseDir)) {
        NES_DEBUG("FilePhysicalStreamsPersistence: creating directories at " << baseDir);
        std::filesystem::create_directories(baseDir);
    }
}

bool FilePhysicalStreamsPersistence::persistConfiguration(SourceConfigPtr sourceConfig) {
    std::string physicalStreamName = sourceConfig->getPhysicalStreamName()->getValue();

    std::filesystem::path fileName(physicalStreamName + ".yml");
    std::filesystem::path filePath = baseDir / fileName;

    NES_DEBUG("FilePhysicalStreamsPersistence::persistConfiguration: persisting configuration to "
              << filePath.c_str() << ", physicalStreamName=" << physicalStreamName);
    try {
        std::ofstream ofs(filePath);
        ofs << sourceConfig->toYaml();
        ofs.close();
        return true;
    } catch (std::exception& ex) {
        NES_ERROR("FilePhysicalStreamsPersistence::persistConfiguration: persisting configuration failed: " << ex.what());
    }
    return false;
}

std::vector<SourceConfigPtr> FilePhysicalStreamsPersistence::loadConfigurations() {
    std::vector<SourceConfigPtr> sourceConfigs{};
    for (const auto& entry : std::filesystem::directory_iterator(baseDir)) {
        if (!entry.is_regular_file() || entry.path().extension() != ".yml") {
            continue;
        }

        try {
            NES_DEBUG("FilePhysicalStreamsPersistence::loadConfigurations: loading configuration from " << entry.path().c_str());
            auto sourceConfig = SourceConfig::create();
            sourceConfig->overwriteConfigWithYAMLFileInput(entry.path());
            sourceConfigs.push_back(sourceConfig);
        } catch (std::exception& ex) {
            NES_ERROR("FilePhysicalStreamsPersistence::loadConfigurations: failed loading: " << ex.what());
            continue;
        }
    }
    return sourceConfigs;
}

}// namespace NES
