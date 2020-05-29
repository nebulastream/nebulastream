#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <utility>

namespace NES {
MemoryLayout::MemoryLayout(NES::PhysicalSchemaPtr physicalSchema) : physicalSchema(std::move(physicalSchema)) {}
}// namespace NES