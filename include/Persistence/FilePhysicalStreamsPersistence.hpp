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

#ifndef NES_FILEPHYSICALSTREAMSPERSISTENCE_H
#define NES_FILEPHYSICALSTREAMSPERSISTENCE_H

#include <Persistence/PhysicalStreamsPersistence.hpp>
#include <filesystem>

namespace NES {
class FilePhysicalStreamsPersistence;
typedef std::shared_ptr<FilePhysicalStreamsPersistence> FilePhysicalStreamsPersistencePtr;

class FilePhysicalStreamsPersistence : public PhysicalStreamsPersistence {
  public:
    FilePhysicalStreamsPersistence(const std::string& baseDir);

    bool persistConfiguration(SourceConfigPtr sourceConfig) override;

    std::vector<SourceConfigPtr> loadConfigurations() override;

  private:
    std::filesystem::path baseDir;
};
}// namespace NES

#endif//NES_FILEPHYSICALSTREAMSPERSISTENCE_H
