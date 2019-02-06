/*
 * APIDataTypes.hpp
 *
 *  Created on: Feb 4, 2019
 *      Author: zeuchste
 */

#ifndef INCLUDE_API_APIDATATYPES_HPP_
#define INCLUDE_API_APIDATATYPES_HPP_

namespace iotdb{

struct APIDataType {
  enum Type { Boolean, Char, String, Int, Uint, Long, Double };
  Type t_;

  APIDataType(Type t) : t_(t) {}

  operator Type() const { return t_; }

  size_t defaultSize() const {
    switch (t_) {
    case APIDataType::String:
      return 255;
    default:
      return 1;
    }
  }

  const std::string cType() const {
    switch (t_) {
    case APIDataType::Boolean:
      return "bool";
    case APIDataType::Char:
      return "char";
    case APIDataType::String:
      return "char";
    case APIDataType::Int:
      return "int";
    case APIDataType::Uint:
      return "unsigned int";
    case APIDataType::Long:
      return "long";
    case APIDataType::Double:
      return "double";
    default:
      throw std::invalid_argument("data type not supported");
    }
  }

  const std::string keyType() const {
    switch (t_) {
    case APIDataType::Boolean:
      return "bool";
    case APIDataType::Char:
    case APIDataType::String:
      return "std::string";
    case APIDataType::Int:
      return "int";
    case APIDataType::Uint:
      return "unsigned int";
    case APIDataType::Long:
      return "long";
    case APIDataType::Double:
      return "double";
    default:
      throw std::invalid_argument("data type not supported");
    }
  }
};
}


#endif /* INCLUDE_API_APIDATATYPES_HPP_ */
