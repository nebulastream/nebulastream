
#include <CodeGen/CodeExpression.hpp>
#include <Core/DataTypes.hpp>
#include <sstream>
#include <string>
#include <vector>
#include <CodeGen/C_CodeGen/Statement.hpp>
#include <CodeGen/C_CodeGen/BinaryOperatorStatement.hpp>

namespace iotdb {

DataType::DataType(){

}

DataType::DataType(const DataType&){

}

DataType& DataType::operator=(const DataType&){
  return *this;
}

const StatementPtr DataType::getStmtCopyAssignment(const AssignmentStatment& aParam) const{
    return VarRef(aParam.lhs_tuple_var)[VarRef(aParam.lhs_index_var)].accessRef(VarRef(aParam.lhs_field_var))
                      .assign(VarRef(aParam.rhs_tuple_var)[VarRef(aParam.rhs_index_var)].accessRef(VarRef(aParam.rhs_field_var)))
                      .copy();
}

DataType::~DataType() {}

AttributeField::AttributeField(const std::string& _name, DataTypePtr _data_type) : name(_name), data_type(_data_type)
{
    // assert(data_type!=nullptr);
}

AttributeField::AttributeField(const std::string& _name, const BasicType& _type)
    : name(_name), data_type(createDataType(_type))
{
}

AttributeField::AttributeField(const std::string& _name, uint32_t _size)
    : name(_name), data_type(createDataTypeVarChar(_size))
{
}
uint32_t AttributeField::getFieldSize() const { return data_type->getSizeBytes(); }

const DataTypePtr AttributeField::getDataType() const { return data_type; }

const std::string AttributeField::toString() const
{

    std::stringstream ss;
    ss << name << ":" << data_type->toString();
    return ss.str();
}

const AttributeFieldPtr AttributeField::copy() const{
  return std::make_shared<AttributeField>(*this);
}

bool AttributeField::isEqual(const AttributeField& attr){
  if(attr.name==name && attr.data_type->toString() == data_type->toString()){
      return true;
  }
  return false;
}

bool AttributeField::isEqual(const AttributeFieldPtr& attr){
  if(!attr) return false;
  return this->isEqual(attr);
}

AttributeField::AttributeField(const AttributeField& other)
  : name(other.name), data_type(other.data_type->copy())
{
}

AttributeField& AttributeField::operator=(const AttributeField& other){
  if(this!=&other){
      this->name=other.name;
      this->data_type=other.data_type->copy();
  }
  return *this;
}

const AttributeFieldPtr createField(const std::string name, const BasicType& type)
{
    AttributeFieldPtr ptr = std::make_shared<AttributeField>(name, type);
    return ptr;
}

const AttributeFieldPtr createField(const std::string name, uint32_t size)
{
    AttributeFieldPtr ptr = std::make_shared<AttributeField>(name, size);
    return ptr;
}

const AttributeFieldPtr createField(const std::string name, DataTypePtr type){
    AttributeFieldPtr ptr = std::make_shared<AttributeField>(name, type);
    return ptr;
}

ValueType::ValueType(){

}
ValueType::ValueType(const ValueType&){

}
ValueType& ValueType::operator=(const ValueType&){
  return *this;
}

ValueType::~ValueType() {}

class BasicValueType : public ValueType {
  public:
    BasicValueType(const BasicType& type, const std::string& value) : type_(type), value_(value){};

    const DataTypePtr getType() const override{ return createDataType(type_); }

    const CodeExpressionPtr getCodeExpression() const override{ return std::make_shared<CodeExpression>(value_); }

    const ValueTypePtr copy() const override{
      return std::make_shared<BasicValueType>(*this);
    }

    const bool isArrayValueType() const override { return false; }

    ~BasicValueType() override;

  private:
    const BasicType type_;
    std::string value_;
};

BasicValueType::~BasicValueType() {}

class BasicDataType : public DataType {
  public:
    BasicDataType(const BasicType& _type) : type(_type), dataSize(0) {}

    BasicDataType(const BasicType& _type, uint32_t _size) : type(_type), dataSize(_size) {}

    ValueTypePtr getDefaultInitValue() const  override{ return ValueTypePtr(); }

    ValueTypePtr getNullValue() const  override{ return ValueTypePtr(); }

    const bool isEqual(DataTypePtr ptr) const override{
        std::shared_ptr<BasicDataType> temp = std::dynamic_pointer_cast<BasicDataType>(ptr);
        if(temp) return isEqual(temp);
        return false;
    }

    const bool isEqual(std::shared_ptr<BasicDataType> btr) const{
        return ((btr->type == this->type) && (btr->getSizeBytes() == this->getSizeBytes()));
    }

    const bool isArrayDataType() const { return false; }

    uint32_t getSizeBytes() const override
    {
        if (dataSize == 0) {
            return getFixSizeBytes();
        }
        else {
            return getFixSizeBytes() * dataSize;
        }
    }

    uint32_t getFixSizeBytes() const
    {
        switch (type) {
        case INT8:
            return sizeof(int8_t);
        case UINT8:
            return sizeof(uint8_t);
        case INT16:
            return sizeof(int16_t);
        case UINT16:
            return sizeof(uint16_t);
        case INT32:
            return sizeof(int32_t);
        case UINT32:
            return sizeof(uint32_t);
        case INT64:
            return sizeof(int64_t);
        case UINT64:
            return sizeof(uint64_t);
        case FLOAT32:
            return sizeof(float);
        case FLOAT64:
            return sizeof(double);
        case BOOLEAN:
            return sizeof(bool);
        case CHAR:
            return sizeof(char);
        case DATE:
            return sizeof(uint32_t);
        case VOID_TYPE:
            return 0;
        }
        return 0;
    }
    const std::string toString() const override
    {
        switch (type) {
        case INT8:
            return "INT8";
        case UINT8:
            return "UINT8";
        case INT16:
            return "INT16";
        case UINT16:
            return "UINT16";
        case INT32:
            return "INT32";
        case UINT32:
            return "UINT32";
        case INT64:
            return "INT64";
        case UINT64:
            return "UINT64";
        case FLOAT32:
            return "FLOAT32";
        case FLOAT64:
            return "FLOAT64";
        case BOOLEAN:
            return "BOOLEAN";
        case CHAR:
            return "CHAR";
        case DATE:
            return "DATE";
        case VOID_TYPE:
            return "VOID";
        }
        return "";
    }

    const std::string convertRawToString(void* data) const override
    {
        if (!data)
            return std::string();
        std::stringstream str;
        switch (type) {
        case INT8:
            return std::to_string(*reinterpret_cast<int8_t*>(data));
        case UINT8:
            return std::to_string(*reinterpret_cast<uint8_t*>(data));
        case INT16:
            return std::to_string(*reinterpret_cast<int16_t*>(data));
        case UINT16:
            return std::to_string(*reinterpret_cast<uint16_t*>(data));
        case INT32:
            return std::to_string(*reinterpret_cast<int32_t*>(data));
        case UINT32:
            return std::to_string(*reinterpret_cast<uint32_t*>(data));
        case INT64:
            return std::to_string(*reinterpret_cast<int64_t*>(data));
        case UINT64:
            return std::to_string(*reinterpret_cast<uint64_t*>(data));
        case FLOAT32:
            return std::to_string(*reinterpret_cast<float*>(data));
        case FLOAT64:
            return std::to_string(*reinterpret_cast<double*>(data));
        case BOOLEAN:
            return std::to_string(*reinterpret_cast<bool*>(data));
        case CHAR:
            if (dataSize == 0) {
                str << static_cast<char>(*static_cast<char*>(data));
            }
            else {
                str << static_cast<char>(*static_cast<char*>(data));
            }
            return str.str();
        case DATE:
            str << *static_cast<char*>(data);
            return str.str();
        case VOID_TYPE:
            return "";
        }
        return "";
    }

    const CodeExpressionPtr getDeclCode(const std::string& identifier) const override
    {
        std::stringstream str;
        str << " " << identifier;
        return combine(getCode(), std::make_shared<CodeExpression>(str.str()));
    }

    const CodeExpressionPtr getCode() const override
    {
        switch (type) {
        case INT8:
            return std::make_shared<CodeExpression>("int8_t");
        case UINT8:
            return std::make_shared<CodeExpression>("uint8_t");
        case INT16:
            return std::make_shared<CodeExpression>("int16_t");
        case UINT16:
            return std::make_shared<CodeExpression>("uint16_t");
        case INT32:
            return std::make_shared<CodeExpression>("int32_t");
        case UINT32:
            return std::make_shared<CodeExpression>("uint32_t");
        case INT64:
            return std::make_shared<CodeExpression>("int64_t");
        case UINT64:
            return std::make_shared<CodeExpression>("uint64_t");
        case FLOAT32:
            return std::make_shared<CodeExpression>("float");
        case FLOAT64:
            return std::make_shared<CodeExpression>("double");
        case BOOLEAN:
            return std::make_shared<CodeExpression>("bool");
        case CHAR:
            return std::make_shared<CodeExpression>("char");
        case DATE:
            return std::make_shared<CodeExpression>("uint32_t");
        case VOID_TYPE:
            return std::make_shared<CodeExpression>("void");
        }
        return nullptr;
    }

    const CodeExpressionPtr getTypeDefinitionCode() const override{ return std::make_shared<CodeExpression>(""); }

    const DataTypePtr copy() const override{
      return std::make_shared<BasicDataType>(*this);
    }

    ~BasicDataType() override;

  private:
    BasicType type;
    uint32_t dataSize;
};

BasicDataType::~BasicDataType() {}

class PointerDataType : public DataType {
  public:
    PointerDataType(const DataTypePtr& type) : DataType(), base_type_(type) {}
    ValueTypePtr getDefaultInitValue() const override{ return ValueTypePtr(); }

    ValueTypePtr getNullValue() const override{ return ValueTypePtr(); }
    uint32_t getSizeBytes() const override
    {
        /* assume a 64 bit architecture, each pointer is 8 bytes */
        return 8;
    }
    const std::string toString() const override{ return base_type_->toString() + "*"; }
    const std::string convertRawToString(void* data) const override
    {
        if (!data)
            return "";
        return "POINTER"; // std::to_string(data);
    }
    const bool isArrayDataType() const { return false; }

    const CodeExpressionPtr getDeclCode(const std::string& identifier) const override
    {
        return std::make_shared<CodeExpression>(base_type_->getCode()->code_ + "* " + identifier);
    }

    const CodeExpressionPtr getCode() const override
    {
        return std::make_shared<CodeExpression>(base_type_->getCode()->code_ + "*");
    }

    /*
    const CodeExpressionPtr getCodeDeclaration(const std::string& identifier) const override
    {
        return std::make_shared<CodeExpression>("char "+base_type_->getCodeDeclaration(identifier)->code_ + "[25]");
    }*/

    const bool isEqual(DataTypePtr ptr) const override{
        std::shared_ptr<PointerDataType> temp = std::dynamic_pointer_cast<PointerDataType>(ptr);
        if(temp) return isEqual(temp);
        return false;
    }

    const bool isEqual(std::shared_ptr<PointerDataType> btr) const {
        return base_type_->isEqual(btr->base_type_);
    }

    const CodeExpressionPtr getTypeDefinitionCode() const override{ return base_type_->getTypeDefinitionCode(); }

    const DataTypePtr copy() const override{
      return std::make_shared<PointerDataType>(*this);
    }

    virtual ~PointerDataType() override;

  private:
    DataTypePtr base_type_;
};

PointerDataType::~PointerDataType() {}

const DataTypePtr createDataType(const BasicType& type)
{
    DataTypePtr ptr = std::make_shared<BasicDataType>(type);
    return ptr;
}

const DataTypePtr createVarDataType(const BasicType& type, uint32_t size)
{
    DataTypePtr ptr = std::make_shared<BasicDataType>(type, size);
    return ptr;
}

const DataTypePtr createPointerDataType(const DataTypePtr& type) { return std::make_shared<PointerDataType>(type); }

const DataTypePtr createPointerDataType(const BasicType& type)
{
    return std::make_shared<PointerDataType>(createDataType(type));
}

const DataTypePtr createDataTypeVarChar(const uint32_t& size)
{
    DataTypePtr ptr = std::make_shared<BasicDataType>(CHAR, size);
    return ptr;
}

const ValueTypePtr createBasicTypeValue(const BasicType& type, const std::string& value)
{

    /** \todo: create instance of datatype and add a parseValue() method to datatype, so we can check whether the value
     * inside the string matches the type */
    return std::make_shared<BasicValueType>(type, value);
}




//todo: ------------------------------

    class ArrayDataType;
    typedef std::shared_ptr<ArrayDataType> ArrayDataTypePtr;

    class ArrayValueType;
    typedef std::shared_ptr<ArrayValueType> ArrayValueTypePtr;

    class ArrayDataType : public DataType {
    public:
        ArrayDataType (DataTypePtr ptr, u_int32_t dimension) : DataType(), _data(ptr), _dimension(dimension) {}
        //ArrayDataType (const BasicType& basictype, u_int32_t dimension) : DataType(), _data(std::make_shared<DataType>(basictype)), _dimension(dimension) {}

        ValueTypePtr getDefaultInitValue() const override { return ValueTypePtr(); }
        ValueTypePtr getNullValue() const override { return ValueTypePtr(); }
        uint32_t getSizeBytes() const override { return (_dimension * _data->getSizeBytes()); }
        const std::string toString() const override {
            std::stringstream sstream;
            sstream << _data->toString() << "[" << _dimension << "]";
            return sstream.str();
        }
        const std::string convertRawToString(void* data) const override{
            if(!data) return "";
            uint32_t i;
            char* pointer = static_cast<char*>(data);
            std::stringstream str;
            uint32_t step = _data->getSizeBytes() / _dimension;
            if(!isCharDataType()) str << '[';
            for(i = 0; i < _dimension; i++) {
                char temp[step];
                temp[0] = pointer[step * i];
                if ((i != 0) && !isCharDataType()) str << ", ";
                str << _data->convertRawToString(temp);
            }
            if(!isCharDataType()) str << ']';
            return str.str();
        }


        const CodeExpressionPtr getDeclCode(const std::string& identifier) const override {
            CodeExpressionPtr ptr;
            if(identifier != ""){
                ptr = _data->getCode();
                ptr = combine(ptr, std::make_shared<CodeExpression>(" " + identifier));
                ptr = combine(ptr, std::make_shared<CodeExpression>("["));
                ptr = combine(ptr, std::make_shared<CodeExpression>(std::to_string(_dimension)));
                ptr = combine(ptr, std::make_shared<CodeExpression>("]"));
            } else {
                ptr = _data->getCode();
            }
            return ptr;
        }

        virtual const StatementPtr getStmtCopyAssignment(const AssignmentStatment& aParam) const override {
            FunctionCallStatement func_call("memcpy");
            func_call.addParameter(VarRef(aParam.lhs_tuple_var)[VarRef(aParam.lhs_index_var)].accessRef(VarRef(aParam.lhs_field_var)));
            func_call.addParameter(VarRef(aParam.rhs_tuple_var)[VarRef(aParam.rhs_index_var)].accessRef(VarRef(aParam.rhs_field_var)));
            func_call.addParameter(ConstantExprStatement(createBasicTypeValue(UINT64, std::to_string(this->_dimension))));
            return func_call.copy();
        }

        const CodeExpressionPtr getCode() const override {
            std::stringstream str;
            str << "[" << _dimension << "]";
            return combine(_data->getCode(), std::make_shared<CodeExpression>(str.str()));
        }

        const bool isEqual(DataTypePtr ptr) const override { return (this->toString().compare(ptr->toString()) == 0); };
        const bool isCharDataType() const { return _data->isEqual(BasicDataType(BasicType::CHAR).copy()); }
        const bool isArrayDataType() const override { return true; }

        const CodeExpressionPtr getTypeDefinitionCode() const override{ return std::make_shared<CodeExpression>(""); }

        const DataTypePtr copy() const override { return std::make_shared<ArrayDataType>(*this); }
        virtual ~ArrayDataType() override;
    private:
        DataTypePtr _data;
        u_int32_t _dimension;
    };

    ArrayDataType::~ArrayDataType() {}

/**
 * class ArrayValueType keeps a field of values of basic types
 */
    class ArrayValueType : public ValueType {
    public:
        ArrayValueType(const ArrayDataType& type, const std::vector<std::string>& value) : type_(std::make_shared<ArrayDataType>(type)), value_(value){};
        ArrayValueType(const ArrayDataType& type, const std::string& value) : type_(std::make_shared<ArrayDataType>(type)), isString_(true){
            value_.push_back(value);
        };

        const DataTypePtr getType() const override{ return type_; }

        const CodeExpressionPtr getCodeExpression() const override{
            std::stringstream str;
            if(isString_) {
                str << "\"" << value_.at(0) << "\"";
                return std::make_shared<CodeExpression>(str.str());
            }
            bool isCharArray = (type_->isCharDataType());
            str << "{";
            u_int32_t i;
            for(i = 0; i < value_.size(); i++){
                if(i != 0) str << ", ";
                if(isCharArray) str << "\'";
                str << value_.at(i);
                if(isCharArray) str << "\'";
            }
            str << "}";
            return std::make_shared<CodeExpression>(str.str());
        }

        const ValueTypePtr copy() const override{
            return std::make_shared<ArrayValueType>(*this);
        }

        const bool isArrayValueType() const override{ return true; }

        virtual ~ArrayValueType() override;

    private:
        const ArrayDataTypePtr type_;
        bool isString_ = false;
        std::vector<std::string> value_;
    };

    ArrayValueType::~ArrayValueType() {}



const DataTypePtr createArrayDataType(const BasicType& type, uint32_t dimension)
{
    return std::make_shared<ArrayDataType>(ArrayDataType(BasicDataType(type).copy(), dimension));
}
/**
 * creates a "string"-value (means char *)
 * @param value : std:string : the string value
 * @return ValueTypePtr : the structure keeping the given values (-- here it keeps it as a single string)
 */
const ValueTypePtr createStringValueType(const std::string& value)
{
    return std::make_shared<ArrayValueType>(ArrayDataType(BasicDataType(BasicType::CHAR).copy(), value.size()), value);
}

const ValueTypePtr createStringValueType(const char* value, u_int16_t dimension)
{
    u_int32_t i = 0;
    std::stringstream str;
    /**
     * caused by while-loop it could make sense to include a maximum string-size here.
     * ERROR-POSSIBILITY: INFINITE-LOOP - CHARPOINTER WITHOUT PREDEFINED SIZE DOES NOT END WITH '\0'
     */
    if(dimension == 0) while(value[i] != '\0'){
        str << value[i];
        i++;
    } else {
        u_int32_t j;
        for (j = 0; j < dimension; j++) {
            str << value[j];
            if(value[j] == '\0') break;
        }
        if (j == dimension){
            str << '\0';
            j++;
        }
        i = j;
    }

    return std::make_shared<ArrayValueType>(ArrayDataType(BasicDataType(BasicType::CHAR).copy(), i), str.str());
}

const ValueTypePtr createArrayValueType(const BasicType& type, const std::vector<std::string>& value)
{
    /** \todo: as above. Missing type-check for the values*/
    return std::make_shared<ArrayValueType>(ArrayDataType(BasicDataType(type).copy(), value.size()), value);
}

} // namespace iotdb

