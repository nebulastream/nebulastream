#ifndef INCLUDE_CORE_ATTRIBUTEFIELD_HPP_
#define INCLUDE_CORE_ATTRIBUTEFIELD_HPP_

#pragma once

#include <API/Types/DataTypes.hpp>

namespace NES {

class AttributeField {
  public:
    AttributeField() = default;
    AttributeField(const std::string& name, DataTypePtr data_type);
    AttributeField(const std::string& name, const BasicType&);
    AttributeField(const std::string& name);
    AttributeField(const std::string& name, uint32_t dataTypeSize);

    std::string name;
    DataTypePtr data_type;
    uint32_t getFieldSize() const;
    DataTypePtr getDataType() const;
    bool hasType() const;
    const std::string toString() const;

    const AttributeFieldPtr copy() const;

    bool isEqual(const AttributeField& attr);
    bool isEqual(AttributeFieldPtr attr);

    AttributeField(const AttributeField&);
    AttributeField& operator=(const AttributeField&);

    bool operator==(const AttributeField& rhs) const {
        return name == rhs.name && *data_type.get() == *rhs.data_type.get();
    }

  private:
    friend class boost::serialization::access;

    template<class Archive>
    void serialize(Archive& ar, unsigned) {
        ar& BOOST_SERIALIZATION_NVP(name)
            & BOOST_SERIALIZATION_NVP(data_type);
    }
};

}// namespace NES
#endif//INCLUDE_CORE_ATTRIBUTEFIELD_HPP_
