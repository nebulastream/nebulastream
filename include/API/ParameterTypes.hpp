
#pragma once

#include <memory>
#include <vector>

namespace NES {

/**
 * @brief TODO this is deprecated and will be removed as soon as the the new physical operators are in place.
 */

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

class UserAPIExpression;
typedef std::shared_ptr<UserAPIExpression> UserAPIExpressionPtr;

class Field;
typedef std::shared_ptr<Field> FieldPtr;

class Predicate;
typedef std::shared_ptr<Predicate> PredicatePtr;

class Mapper;
typedef std::shared_ptr<Mapper> MapperPtr;

struct Attributes {
    Attributes(AttributeFieldPtr field1);
    Attributes(AttributeFieldPtr field1, AttributeFieldPtr field2);
    Attributes(AttributeFieldPtr field1, AttributeFieldPtr field2, AttributeFieldPtr field3);
    /** \todo add more constructor cases for more fields */
    std::vector<AttributeFieldPtr> attrs;
};

class DataSource;
typedef std::shared_ptr<DataSource> DataSourcePtr;

class SinkMedium;
typedef std::shared_ptr<SinkMedium> DataSinkPtr;

const DataSourcePtr copy(const DataSourcePtr);
const DataSinkPtr copy(const DataSinkPtr);
const PredicatePtr copy(const PredicatePtr);
const Attributes copy(const Attributes&);
const MapperPtr copy(const MapperPtr);

const std::string toString(const DataSourcePtr);
const std::string toString(const DataSinkPtr);
const std::string toString(const PredicatePtr);
const std::string toString(const Attributes&);
const std::string toString(const MapperPtr);

}// namespace NES
