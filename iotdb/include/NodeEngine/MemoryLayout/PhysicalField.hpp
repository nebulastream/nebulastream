#ifndef INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALFIELD_HPP_
#define INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALFIELD_HPP_

#include <memory>
#include <NodeEngine/TupleBuffer.hpp>
#include <API/Types/DataTypes.hpp>
#include <Util/Logger.hpp>
namespace NES {
class PhysicalField;
typedef std::shared_ptr<PhysicalField> PhysicalFieldPtr;

template<class ValueType>
class BasicPhysicalField;

class ArrayPhysicalField;

class PhysicalField : public std::enable_shared_from_this<PhysicalField> {
  public:
    PhysicalField() {};
    ~PhysicalField() {};
    const virtual PhysicalFieldPtr copy() const = 0;

    template<class ValueType>
    std::shared_ptr<BasicPhysicalField<ValueType>> asValueField() {
        if(!isFieldOfType<ValueType>()){
            NES_FATAL_ERROR("This field is not of that type");
            throw IllegalArgumentException("This field is not of that type");
        }
        return std::dynamic_pointer_cast<BasicPhysicalField<ValueType>>(this->shared_from_this());
    }

    virtual std::shared_ptr<ArrayPhysicalField> asArrayField() {
        NES_FATAL_ERROR("This field is not an array field");
        throw IllegalArgumentException("This field is not an array field");
    }

    template<class ValueType>
    bool isFieldOfType(){
        if(std::dynamic_pointer_cast<BasicPhysicalField<ValueType>>(this->shared_from_this())){
            return true;
        };
        return false;
    }

};


PhysicalFieldPtr createArrayPhysicalField(DataTypePtr componentField, uint64_t bufferOffset);

}
#endif //INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALFIELD_HPP_
