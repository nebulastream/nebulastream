//
// Created by pgrulich on 23.06.22.
//

#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_TYPES_BASICTYPES_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_TYPES_BASICTYPES_HPP_


namespace NES::ExecutionEngine::Experimental::IR::Operations {

enum PrimitiveStamp {
    //BasicTypes
    // Type < 5 is INT
    INT1    = 0,
    INT8    = 1,
    INT16   = 2,
    INT32   = 3,
    INT64   = 4,
    // Type < 10 is UINT
    UINT1    = 5,
    UINT8    = 6,
    UINT16   = 7,
    UINT32   = 8,
    UINT64   = 9,

    // Type < 12 is Float
    FLOAT   = 10,
    DOUBLE  = 11,

    BOOLEAN = 12,
    CHAR    = 13,
    VOID    = 14,

    // Pointer Types
    INT8PTR = 15,

    //DerivedTypes
    ARRAY     = 32,
    CHARARRAY = 33,
    STRUCT    = 34
};

}
#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_TYPES_BASICTYPES_HPP_
