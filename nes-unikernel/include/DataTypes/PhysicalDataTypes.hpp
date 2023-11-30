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

#ifndef NES_PHYSICALDATATYPES_HPP
#define NES_PHYSICALDATATYPES_HPP

template<typename CType>
struct SimplePhysicalDataType {
    static constexpr size_t size = sizeof(CType);
    using ctype = CType;
};

#define SIMPLE_DT(name, ctype)                                                                                                   \
    struct name : public SimplePhysicalDataType<ctype> {}

SIMPLE_DT(UINT8, uint8_t);
SIMPLE_DT(UINT16, uint16_t);
SIMPLE_DT(UINT32, uint32_t);
SIMPLE_DT(UINT64, uint64_t);

SIMPLE_DT(INT8, int8_t);
SIMPLE_DT(INT16, int16_t);
SIMPLE_DT(INT32, int32_t);
SIMPLE_DT(INT64, int64_t);

SIMPLE_DT(FLOAT32, float);
SIMPLE_DT(FLOAT64, double);

SIMPLE_DT(CHAR, char);
SIMPLE_DT(BOOLEAN, bool);

// Var char is represented as an int
SIMPLE_DT(TEXT, u_int32_t);

#endif//NES_PHYSICALDATATYPES_HPP
