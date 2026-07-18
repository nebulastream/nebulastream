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

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{

class PagedVector;
class PagedVectorRef;

/// Opaque owning handle for a Nautilus-compiled Record comparator.
class PagedVectorComparator
{
private:
    using Function = std::function<bool(TupleBuffer*, int8_t*, TupleBuffer*, int8_t*)>;

    explicit PagedVectorComparator(Function function) : function(std::move(function)) { }

    static std::shared_ptr<PagedVectorComparator> create(Function function)
    {
        return std::shared_ptr<PagedVectorComparator>{new PagedVectorComparator(std::move(function))};
    }

    [[nodiscard]] bool compare(TupleBuffer* lhsPage, int8_t* lhsTuple, TupleBuffer* rhsPage, int8_t* rhsTuple) const
    {
        return function(lhsPage, lhsTuple, rhsPage, rhsTuple);
    }

    Function function;

    friend class PagedVector;
    friend class PagedVectorRef;
};

}
