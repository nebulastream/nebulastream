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

#include <Execution/Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Nautilus/Backends/CPP/CPPLoweringProvider.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/Util/CompilationOptions.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <Stage/CppBackendExport.hpp>
#include <Stage/QueryPipeliner.hpp>
#include <Util/DumpHelper.hpp>
#include <Util/Timer.hpp>
#include <boost/filesystem/fstream.hpp>
#include <string>

namespace NES::Unikernel::Export {
template<CppBackendExport::Function f>
std::string CppBackendExport::getCppSource(const std::shared_ptr<NES::Runtime::Execution::PhysicalOperatorPipeline>& pipeline,
                                           NES::DumpHelper& dh,
                                           NES::Timer<>& timer,
                                           NES::PipelineId pipelineId) {
    dh.change_prefix(fmt::format("{}-{}", magic_enum::enum_name(f), pipelineId));

    auto executableStage =
        NES::Runtime::Execution::CompiledExecutablePipelineStage(pipeline, "", NES::Nautilus::CompilationOptions());

    std::shared_ptr<NES::Nautilus::IR::IRGraph> ir = nullptr;
    if constexpr (f == setup) {
        ir = executableStage.setupIR();
    } else if constexpr (f == execute) {
        ir = executableStage.createIR(dh, timer);
    } else if constexpr (f == terminate) {
        ir = executableStage.closeIR();
    } else {
        NES_NOT_IMPLEMENTED();
    }

    dh.dump("ir.txt", ir->toString());
    auto cppSource = NES::Nautilus::Backends::CPP::CPPLoweringProvider::lower(ir, magic_enum::enum_name(f));
    dh.dump(".cpp", cppSource);
    return cppSource;
}

std::string CppBackendExport::emitAsCppSourceFiles(Stage stage) {
    auto pipeline = stage.pipeline;
    auto pipelineRoots = pipeline->getQueryPlan()->getRootOperators();
    NES_ASSERT(pipelineRoots.size() == 1, "A pipeline should have a single root operator.");
    auto rootOperator = pipelineRoots[0];
    auto nautilusPipeline = rootOperator->as<NES::QueryCompilation::NautilusPipelineOperator>();

    NES::Nautilus::CompilationOptions options;
    auto identifier = fmt::format("NautilusCompilation-{}-{}-{}",
                                  pipeline->getQueryPlan()->getQueryId(),
                                  pipeline->getQueryPlan()->getQuerySubPlanId(),
                                  pipeline->getPipelineId());
    options.setIdentifier(identifier);

    // enable dump to console if the compiler options are set
    options.setDumpToConsole(false);

    // enable dump to file if the compiler options are set
    options.setDumpToFile(true);

    //TODO: Enable this
    options.setProxyInlining(false);

    auto dumpHelper = NES::DumpHelper::create(options.getIdentifier(),
                                              options.isDumpToConsole(),
                                              options.isDumpToFile(),
                                              options.getDumpOutputPath());

    NES::Timer timer("CompilationBasedPipelineExecutionEngine " + options.getIdentifier());
    timer.start();
    auto setupSource = getCppSource<setup>(nautilusPipeline->getNautilusPipeline(), dumpHelper, timer, pipeline->getPipelineId());

    auto executeSource =
        getCppSource<execute>(nautilusPipeline->getNautilusPipeline(), dumpHelper, timer, pipeline->getPipelineId());

    auto terminateSource =
        getCppSource<terminate>(nautilusPipeline->getNautilusPipeline(), dumpHelper, timer, pipeline->getPipelineId());

    std::stringstream ss;
    ss << "#include <ProxyFunctions.hpp>\n";
    ss << "#pragma GCC diagnostic push\n";
    ss << "#pragma GCC diagnostic ignored \"-Wunused-parameter\"\n";
    ss << "#pragma GCC diagnostic ignored \"-Wunused-label\"\n";
    ss << "#pragma GCC diagnostic ignored \"-Wtautological-compare\"\n";
    ss << setupSource << '\n';
    ss << executeSource << '\n';
    ss << terminateSource << '\n';

    ss << "#pragma GCC diagnostic pop\n";
    ss << "template<>\n"
       << "void NES::Unikernel::Stage<" << pipeline->getPipelineId()
       << ">::terminate(NES::Unikernel::UnikernelPipelineExecutionContext & context,\n"
       << "NES::Runtime::WorkerContext * workerContext) {\n"
       << "           ::terminate(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext));\n"
       << "}\n";

    ss << "template<>\n"
       << "void NES::Unikernel::Stage<" << pipeline->getPipelineId()
       << ">::setup(NES::Unikernel::UnikernelPipelineExecutionContext & context,\n"
       << "NES::Runtime::WorkerContext * workerContext) {\n"
       << "           ::setup(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext));\n"
       << "}\n";

    ss << "template<>\n"
       << "void NES::Unikernel::Stage<" << pipeline->getPipelineId()
       << ">::execute(NES::Unikernel::UnikernelPipelineExecutionContext & context,\n"
       << "NES::Runtime::WorkerContext * workerContext, NES::Runtime::TupleBuffer& buffer) {\n"
       << "           ::execute(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext), "
          "reinterpret_cast<uint8_t*>(&buffer));\n"
       << "}\n";
    return ss.str();
}
}// namespace NES::Unikernel::Export