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

#include <string>

#include <QueryCompiler/CCodeGenerator/Declarations/FunctionDeclaration.hpp>
#include <QueryCompiler/CCodeGenerator/FileBuilder.hpp>

namespace NES {

FileBuilder FileBuilder::create(const std::string&) {
    FileBuilder builder;
    builder.declations << "#include <cstdint>" << std::endl;
    builder.declations << "#include <string.h>" << std::endl;
    builder.declations << "#include <State/StateVariable.hpp>" << std::endl;
    builder.declations << "#include <Windowing/LogicalWindowDefinition.hpp>" << std::endl;
    builder.declations << "#include <Windowing/WindowHandler/AggregationWindowHandler.hpp>" << std::endl;
    builder.declations << "#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>" << std::endl;
    builder.declations << "#include <Windowing/Runtime/WindowManager.hpp>" << std::endl;
    builder.declations << "#include <Windowing/Runtime/WindowSliceStore.hpp>" << std::endl;
    builder.declations << "#include <NodeEngine/TupleBuffer.hpp>" << std::endl;
    builder.declations << "#include <NodeEngine/WorkerContext.hpp>" << std::endl;
    builder.declations << "#include <NodeEngine/Execution/PipelineExecutionContext.hpp>" << std::endl;
    builder.declations << "#include <NodeEngine/Execution/ExecutablePipelineStage.hpp>" << std::endl;
    builder.declations << "#include <Windowing/WindowHandler/JoinHandler.hpp>" << std::endl;
    return builder;
}
FileBuilder& FileBuilder::addDeclaration(DeclarationPtr declaration) {
    declations << declaration->getCode() << ";";
    return *this;
}
CodeFile FileBuilder::build() {
    CodeFile file;
    file.code = declations.str();
    return file;
}

}// namespace NES
