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

#ifndef NAUTILUSSTAGEEXPORTER_HPP
#define NAUTILUSSTAGEEXPORTER_HPP
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Stage/QueryPipeliner.hpp>
#include <Util/DumpHelper.hpp>
#include <Util/Timer.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <string>

namespace NES::QueryCompilation {
class NautilusPipelineOperator;
}

namespace NES::Unikernel::Export {
class CppBackendExport {

    enum Function {
        setup,
        execute,
        terminate,
    };

    template<Function f>
    std::string filename(const NES::QueryCompilation::OperatorPipelinePtr& pipeline) {
        return fmt::format("{}{}.cpp", magic_enum::enum_name(f), pipeline->getPipelineId());
    }

    template<Function f>
    std::string getCppSource(const std::shared_ptr<NES::Runtime::Execution::PhysicalOperatorPipeline>& pipeline,
                             NES::DumpHelper& dh,
                             Timer<>& timer,
                             NES::PipelineId pipelineId);

  public:
    std::string emitAsCppSourceFiles(Stage stage);
};

}// namespace NES::Unikernel::Export

#endif//NAUTILUSSTAGEEXPORTER_H
