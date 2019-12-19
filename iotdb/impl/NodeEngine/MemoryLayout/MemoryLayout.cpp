#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <utility>

namespace iotdb{
 MemoryLayout::MemoryLayout(iotdb::PhysicalSchemaPtr physicalSchema): physicalSchema(std::move(physicalSchema)) {}
}