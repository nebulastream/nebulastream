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
#pragma once

#include <filesystem>
#include <SystestSources/SourceTypes.hpp>

namespace NES
{

class SourceDataProvider
{
public:
    static SystestSourceYAMLBinder::PhysicalSource provideFileDataSource(
        SystestSourceYAMLBinder::PhysicalSource initialPhysicalSourceConfig,
        SystestAttachSource attachSource,
        std::filesystem::path testDataDir);
    static SystestSourceYAMLBinder::PhysicalSource provideInlineDataSource(
        SystestSourceYAMLBinder::PhysicalSource initialPhysicalSourceConfig,
        SystestAttachSource attachSource,
        std::filesystem::path testFilePath);
    static SystestSourceYAMLBinder::PhysicalSource
    provideGeneratorDataSource(SystestSourceYAMLBinder::PhysicalSource initialPhysicalSourceConfig, SystestAttachSource attachSource);
};

}
