#ifndef INCLUDE_NODEENGINE_MEMORYLAYOUT_ROWLAYOUT_HPP_
#define INCLUDE_NODEENGINE_MEMORYLAYOUT_ROWLAYOUT_HPP_

#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
namespace NES {

/**
 * @brief Rowlayout every field of a tuple is stores sequentially.
 */
class RowLayout : public MemoryLayout {
  public:
    RowLayout(PhysicalSchemaPtr physicalSchema);

    uint64_t getFieldOffset(uint64_t recordIndex, uint64_t fieldIndex) override;

    MemoryLayoutPtr copy() const override;
};

}// namespace NES

#endif//INCLUDE_NODEENGINE_MEMORYLAYOUT_ROWLAYOUT_HPP_
