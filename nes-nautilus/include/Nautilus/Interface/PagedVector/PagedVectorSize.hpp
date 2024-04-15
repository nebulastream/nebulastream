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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORSIZE_H
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORSIZE_H
#include <Nautilus/Interface/PagedVector/PagedVectorRowLayout.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES::Nautilus::Interfaces {
class PagedVectorSize {
  public:
#ifndef UNIKERNEL_LIB
    PagedVectorSize(const Schema& schema, size_t pageSize = Runtime::detail::EntryPage::ENTRY_PAGE_SIZE);
#endif
    PagedVectorSize(size_t page_size, size_t size, size_t schema_size);
    [[nodiscard]] size_t size() const;
    [[nodiscard]] size_t schemaSize() const;
    [[nodiscard]] size_t pageSize() const;

  private:
    size_t pageSize_;
    size_t size_;
    size_t schemaSize_;
};
}// namespace NES::Nautilus::Interfaces

#endif//NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORSIZE_H
