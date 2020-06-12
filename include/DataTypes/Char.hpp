#ifndef NES_INCLUDE_DATATYPES_CHAR_HPP_
#define NES_INCLUDE_DATATYPES_CHAR_HPP_

#include <DataTypes/DataType.hpp>
namespace NES {

class Char : public DataType {
  public:
    explicit Char(uint64_t length);
    bool isChar() override;
    uint64_t getLength() const;
    bool isEquals(DataTypePtr otherDataType) override;

  private:
    const uint64_t length;

};

}// namespace NES


#endif//NES_INCLUDE_DATATYPES_CHAR_HPP_
