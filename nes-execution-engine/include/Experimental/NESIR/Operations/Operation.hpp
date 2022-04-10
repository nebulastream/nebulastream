/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_OPERATION_HPP
#define NES_OPERATION_HPP

#include <memory>

namespace NES {

class Operation {
  public:
    enum BasicType {
        //BasicTypes
        // Type < 5 is INT
        INT1    = 0,
        INT8    = 1,
        INT16   = 2,
        INT32   = 3,
        INT64   = 4,
        // Type < 7 is Float
        FLOAT   = 5,
        DOUBLE  = 6,

        BOOLEAN = 7,
        CHAR    = 8,
        VOID    = 9,

        // Pointer Types
        INT8PTR = 10,

        //DerivedTypes
        ARRAY     = 32,
        CHARARRAY = 33,
        STRUCT    = 34
    };
    enum ProxyCallType{
        GetNumTuples = 0, 
        GetDataBuffer = 1,
        Other = 50
    };
    enum OperationType{LoopOp, AddOp, LoadOp, StoreOp, PredicateOp, ConstantOp, AddressOp, FunctionOp, BranchOp, IfOp, 
                       CompareOp, ReturnOp, ProxyCallOp};

    explicit Operation(OperationType opType);
    virtual ~Operation() = default;

    OperationType getOperationType() const;

  private:
    OperationType opType;
};
using OperationPtr = std::shared_ptr<Operation>;

}// namespace NES
#endif//NES_OPERATION_HPP
