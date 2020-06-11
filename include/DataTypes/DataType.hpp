#ifndef NES_INCLUDE_DATATYPES_DATATYPE_HPP_
#define NES_INCLUDE_DATATYPES_DATATYPE_HPP_

#include <memory>

namespace NES {

std::shared_ptr<DataType> DataTypePtr;


class DataType {

    virtual bool isUndefined();
    virtual bool isNumeric();
    virtual bool isInteger();
    virtual bool isFloat();
    virtual bool isArray();
    virtual bool isChar();

    bool isEquals(DataTypePtr otherDataType);
    DataTypePtr join(DataTypePtr otherDataType);

};

std::shared_ptr<DataType> DataTypePtr;


}// namespace NES

#endif//NES_INCLUDE_DATATYPES_DATATYPE_HPP_
