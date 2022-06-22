//
// Created by pgrulich on 08.06.22.
//
#include <Experimental/NESIR/Operations/BranchOperation.hpp>
#include <Experimental/NESIR/Operations/IfOperation.hpp>
#include <Experimental/NESIR/Operations/LoopOperation.hpp>
#include <Experimental/NESIR/Util/NesIrVizDumpHandler.hpp>
#include <filesystem>
#include <set>

namespace NES::ExecutionEngine::Experimental::IR {
NesIrVizDumpHandler::NesIrVizDumpHandler(const std::string& rootDir) : VizDumpHandler(rootDir) {}

void dumpBlock(std::stringstream& ss,
               std::stringstream& edges,
               std::shared_ptr<BasicBlock> block,
               std::set<std::shared_ptr<BasicBlock>>& visitedBlocks) {
    if (visitedBlocks.contains(block))
        return;
    std::stringstream blockObject;
    visitedBlocks.emplace(block);
    blockObject << "{'id':'" << block << "', 'name' : '" << block->getIdentifier() << "', 'children':[";
    for (auto op : block->getOperations()) {
        blockObject << "{'id': '" << op << "', 'name': '" << op->toString() << "' }\n, ";
    }
    blockObject << "]},";
    ss << blockObject.str();
    if (block->getTerminatorOp()->getOperationType() == Operations::Operation::LoopOp) {
        auto loopOp = std::dynamic_pointer_cast<Operations::LoopOperation>(block->getTerminatorOp());
        edges << "{ id: '" << block << "_" << loopOp->getLoopHeadBlock() << "', source: '" << block << "', target: '"
              << loopOp->getLoopHeadBlock() << "' },";
        dumpBlock(ss, edges, loopOp->getLoopHeadBlock(), visitedBlocks);
    } else if (block->getTerminatorOp()->getOperationType() == Operations::Operation::IfOp) {
        auto ifOp = std::dynamic_pointer_cast<Operations::IfOperation>(block->getTerminatorOp());
        if (ifOp->getThenBranchBlock() != nullptr) {
            edges << "{ id: '" << block << "_" << ifOp->getThenBranchBlock() << "', source: '" << block << "', target: '"
                  << ifOp->getThenBranchBlock() << "' },";

            dumpBlock(ss, edges, ifOp->getThenBranchBlock(), visitedBlocks);
        }
        if (ifOp->getElseBranchBlock() != nullptr) {
            edges << "{ id: '" << block << "_" << ifOp->getElseBranchBlock() << "', source: '" << block << "', target: '"
                  << ifOp->getElseBranchBlock() << "' },";
            dumpBlock(ss, edges, ifOp->getElseBranchBlock(), visitedBlocks);
        }
    } else if (block->getTerminatorOp()->getOperationType() == Operations::Operation::BranchOp) {
        auto branchOp = std::dynamic_pointer_cast<Operations::BranchOperation>(block->getTerminatorOp());

        edges << "{ id: '" << block << "_" <<  branchOp->getNextBlock() << "', source: '" << block << "', target: '"
              <<  branchOp->getNextBlock() << "' },";
        dumpBlock(ss, edges, branchOp->getNextBlock(), visitedBlocks);
    }
};

void NesIrVizDumpHandler::dumpIr(std::string context, std::string scope, std::shared_ptr<NESIR> ir) {
    std::stringstream ss;
    std::stringstream edges;
    std::set<BasicBlockPtr> visitedBlocks;
    ss << "{'children':[";
    edges << "'edges':[";

    dumpBlock(ss, edges, ir->getRootOperation()->getFunctionBasicBlock(), visitedBlocks);
    ss << "]," << edges.str() << "]}";
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