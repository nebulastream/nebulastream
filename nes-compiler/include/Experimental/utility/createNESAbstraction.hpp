//#include "NESAbstraction/NESAbstractionNode/NESAbstractionNode.hpp"
#include "../NESAbstraction/NESAbstractionNode/NESAbstractionNode.hpp"
//#include <NESAbstraction/NESAbstractionTree.hpp>
#include "../NESAbstraction/NESAbstractionTree.hpp"


/**
   * @brief Create a simple NESAbstractionTree used for testing.
   * @return std::unique_ptr<NESAbstractionTree>
   */
std::shared_ptr<NESAbstractionTree> createSimpleNESAbstractionTree(
   uint64_t numTuples, std::vector<uint64_t> indexes, std::vector<NESAbstractionNode::BasicType> types);