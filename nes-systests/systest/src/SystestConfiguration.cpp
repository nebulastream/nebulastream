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

#include <SystestConfiguration.hpp>

#include <vector>
#include <Configurations/BaseOption.hpp>

std::vector<NES::Configurations::BaseOption*> NES::Configuration::SystestConfiguration::getOptions()
{
    return {
        &testsDiscoverDir,
        &directlySpecifiedTestFiles,
        &testFileExtension,
        &workingDir,
        &randomQueryOrder,
        &numberConcurrentQueries,
        &testGroups,
        &testDataDir,
        &endlessMode,
        &excludeGroups,
        &grpcAddressUri,
        &timeoutInSeconds};
}
