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

#ifndef NES_E2ETESTRUN_HPP
#define NES_E2ETESTRUN_HPP

#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <DataGeneration/DataGenerator.hpp>
#include <DataProvider/DataProvider.hpp>

namespace NES::Benchmark{
class E2ETestRun {

  public:
    void createCoordinatorConfig();

    void createSources();

    void runQuery(std::string query);

    void removeQuery();

    void writeMeasurementsToCsv();

  private:
    NES::Configurations::CoordinatorConfigurationPtr coordinatorConf;
    NES::Configurations::WorkerConfigurationPtr workerConf;
    DataGeneration::DataGeneratorPtr dataGenerator;
    DataProviding::DataProviderPtr dataProvider;
    QueryId queryId;
    Measurements::Measurements

};
}


#endif//NES_E2ETESTRUN_HPP
