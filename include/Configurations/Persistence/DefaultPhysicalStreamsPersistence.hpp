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

#ifndef NES_DEFAULTPHYSICALSTREAMSPERSISTENCE_H
#define NES_DEFAULTPHYSICALSTREAMSPERSISTENCE_H
#include <Configurations/Persistence/PhysicalStreamsPersistence.hpp>
namespace NES {
class DefaultPhysicalStreamsPersistence;
typedef std::shared_ptr<DefaultPhysicalStreamsPersistence> DefaultPhysicalStreamsPersistencePtr;

/**
 * The default physical stream persistence that does not persist any changes
 */
class DefaultPhysicalStreamsPersistence : public PhysicalStreamsPersistence {
  public:
    static DefaultPhysicalStreamsPersistencePtr create();

    bool persistConfiguration(SourceConfigPtr sourceConfig) override;

    std::vector<SourceConfigPtr> loadConfigurations() override;
};
}// namespace NES

#endif//NES_DEFAULTPHYSICALSTREAMSPERSISTENCE_H
