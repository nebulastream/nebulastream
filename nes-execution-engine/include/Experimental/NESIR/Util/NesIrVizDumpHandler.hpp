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

#ifndef NES_NESIR_VIZ_DUMPHANDLER
#define NES_NESIR_VIZ_DUMPHANDLER

#include <Experimental/NESIR/NESIR.hpp>
#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <Nodes/Util/VizDumpHandler.hpp>
#include <vector>
#include <memory>

namespace NES::ExecutionEngine::Experimental::IR {

class NesIrVizDumpHandler : public VizDumpHandler {
  public:
    NesIrVizDumpHandler(const std::string& rootDir);
    void dumpIr(std::string context, std::string scope, std::shared_ptr<NESIR> ir);
    static std::shared_ptr<NesIrVizDumpHandler> createIrDumpHandler();

};

}// namespace NES::ExecutionEngine::Experimental::IR
#endif//NES_DATASOURCE_HPP