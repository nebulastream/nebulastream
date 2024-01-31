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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZEDREF_H
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZEDREF_H

#include <Nautilus/Interface/Record.hpp>

namespace NES::Nautilus::Interface {
class PagedVectorVarSizedRefIter;
class PagedVectorVarSizedRef {
  public:
    PagedVectorVarSizedRef(const Value<MemRef>& pagedVectorVarSizedRef);

    void writeRecord(Record record);

    Record readRecord();

    PagedVectorVarSizedRefIter begin();

    PagedVectorVarSizedRefIter at();

    PagedVectorVarSizedRefIter end();

  private:
    Value<MemRef> pagedVectorVarSizedRef;
};
} //namespace NES::Nautilus::Interface

#endif//NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZEDREF_H
