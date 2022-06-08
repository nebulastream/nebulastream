//
// Created by pgrulich on 08.06.22.
//
#include <Experimental/NESIR/Util/NesIrVizDumpHandler.hpp>
#include <filesystem>

namespace NES::ExecutionEngine::Experimental::IR {
NesIrVizDumpHandler::NesIrVizDumpHandler(const std::string& rootDir) : VizDumpHandler(rootDir) {}

void dumpBlock(std::stringstream& ss, std::shared_ptr<BasicBlock> block) {
    std::stringstream blockObject;
    blockObject << "{'id':'" << block->getIdentifier() << "', 'name' : \"" << block->getIdentifier() << "\", 'children':[";
    for(auto op: block->getOperations()){
            blockObject << "{'id': \"" << op << "\", 'name': 'const' }\n, ";
    }
    blockObject << "]}";
    ss << blockObject.str();
};

void NesIrVizDumpHandler::dumpIr(std::string context, std::string scope, std::shared_ptr<NESIR> ir) {
    std::stringstream ss;

    ss << "{\"nodes\":[";

    dumpBlock(ss, ir->getRootOperation()->getFunctionBasicBlock());
    ss << "], "
          "'edges':[]}";
    writeToFile(context, scope, ss.str());
}

std::shared_ptr<NesIrVizDumpHandler> NesIrVizDumpHandler::createIrDumpHandler() {
    std::string path = std::filesystem::current_path();
    path = path + std::filesystem::path::preferred_separator + "dump";
    if (!std::filesystem::is_directory(path)) {
        std::filesystem::create_directory(path);
    }
    return std::make_shared<NesIrVizDumpHandler>(path);
}
}// namespace NES::ExecutionEngine::Experimental::IR