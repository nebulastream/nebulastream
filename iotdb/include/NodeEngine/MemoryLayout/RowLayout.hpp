#ifndef IOTDB_INCLUDE_NODEENGINE_MEMORYLAYOUT_ROWLAYOUT_HPP_
#define IOTDB_INCLUDE_NODEENGINE_MEMORYLAYOUT_ROWLAYOUT_HPP_

#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
namespace NES {

class RowLayout : public MemoryLayout {
 public:
  RowLayout(PhysicalSchemaPtr physicalSchema);

  uint64_t getFieldOffset(uint64_t recordIndex, uint64_t fieldIndex) override;
  MemoryLayoutPtr copy() const override;
};


}

#endif //IOTDB_INCLUDE_NODEENGINE_MEMORYLAYOUT_ROWLAYOUT_HPP_
