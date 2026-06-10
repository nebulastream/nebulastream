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

#include <cstddef>
#include <cstdint>
#include <variant>
#include <Runtime/TupleBuffer.hpp>
#include <val_details.hpp>
#include <val_ptr.hpp>
#include <val_std.hpp>

/// A NautilusBuffer is a Wrapper around a TupleBuffer. It can come in two different flavours:
/// 1. The BorrowedNautilusBuffer requires that an instance of a TupleBuffer is kept alive outside the nautilus execution. This handle
///    should be accessed by a single thread, as the handle itself is not Threadsafe. A common use case for the Borrowed variant
///    is the initial entry point into the nautilus pipeline which holds the TupleBuffer on the WorkerThreads stack.
///    If these two things can be guaranteed, you should prefer the BorrowedNautilusBuffer, as it does not require additional buffer
///    lifetime management
/// 2. The OwnedNautilusBuffer is allocated within the nautilus execution, and the lifetime is managed by the nautilus execution.
///    A common usecase is keeping a TupleBuffer alive which was allocated in a `nautilus::invoke` or loaded as a child of a different buffer:
///    ```c++
///    OwnedNautilusBuffer owned;
///    nautilus::invoke(+[](AbstractBufferProvider* provider, TupleBuffer* owned){
///         *owned = provider->getBufferBlocking();
///    }, provider, owned.asArg());
///    /// owned keeps the buffer alive, and releases once it goes out of scope
///    ```
/// APIs should be designed to be agnostic to the ownership of a buffer: `NautilusBuffer` is a type-erased variant that can hold both owned
/// and not owned that should be used most of the time.
namespace NES
{
class BorrowedNautilusBuffer;
class NautilusBuffer;

class OwnedNautilusBuffer
{
    nautilus::val<TupleBuffer> buffer;

public:
    /// Creates an owned buffer that holds a reference-counted copy of `source`'s underlying TupleBuffer. Works for both owned and
    /// borrowed sources, and is the way to promote a borrowed buffer into one whose lifetime is managed by the nautilus execution.
    static OwnedNautilusBuffer copy(const NautilusBuffer& source);

    nautilus::val<int8_t*> data();
    [[nodiscard]] nautilus::val<const int8_t*> data() const;

    [[nodiscard]] nautilus::val<size_t> getNumberOfRecords() const;

    nautilus::val<size_t> storeChild(OwnedNautilusBuffer&& child);
    nautilus::val<size_t> storeChild(BorrowedNautilusBuffer&& child);
    [[nodiscard]] OwnedNautilusBuffer getChild(const nautilus::val<size_t>& index) const;
    [[nodiscard]] nautilus::val<bool> isValid() const;

    /// Returns a pointer to the wrapped TupleBuffer that is only valid for as long as this buffer is alive.
    /// It is solely intended to be passed as an argument to a `nautilus::invoke` and MUST NOT be stored or used to outlive this buffer.
    /// The lvalue-ref qualifier prevents calling it on a temporary, which would return a dangling pointer into the destroyed buffer.
    [[nodiscard]] nautilus::val<const TupleBuffer*> asArg() const&;
    nautilus::val<TupleBuffer*> asArg() &;
    /// Do not call asArg() on a temporary OwnedNautilusBuffer: the returned pointer would dangle.
    [[nodiscard]] nautilus::val<const TupleBuffer*> asArg() const&& = delete;
    nautilus::val<TupleBuffer*> asArg() && = delete;
};

class BorrowedNautilusBuffer
{
    nautilus::val<TupleBuffer*> buffer;

    explicit BorrowedNautilusBuffer(const nautilus::val<TupleBuffer*>& buffer);

public:
    static BorrowedNautilusBuffer from(const nautilus::val<const TupleBuffer*>& originalBuffer);

    nautilus::val<int8_t*> data();
    [[nodiscard]] nautilus::val<const int8_t*> data() const;

    [[nodiscard]] nautilus::val<size_t> getNumberOfRecords() const;

    [[nodiscard]] OwnedNautilusBuffer getChild(const nautilus::val<size_t>& index) const;

    nautilus::val<size_t> storeChild(OwnedNautilusBuffer&& child);

    nautilus::val<size_t> storeChild(BorrowedNautilusBuffer&& child);

    /// Returns a pointer to the wrapped TupleBuffer that is only valid for as long as this buffer is alive.
    /// It is solely intended to be passed as an argument to a `nautilus::invoke` and MUST NOT be stored or used to outlive this buffer.
    [[nodiscard]] nautilus::val<const TupleBuffer*> asArg() const;
    [[nodiscard]] nautilus::val<TupleBuffer*> asArg();
};

class NautilusBuffer
{
    std::variant<BorrowedNautilusBuffer, OwnedNautilusBuffer> underlying;

public:
    [[nodiscard]] nautilus::val<const int8_t*> data() const;

    nautilus::val<int8_t*> data();

    nautilus::val<size_t> storeChild(NautilusBuffer&& buffer);

    /// Loading a child buffer always yields an owned buffer, as its lifetime must be managed by the nautilus execution.
    [[nodiscard]] OwnedNautilusBuffer getChild(const nautilus::val<size_t>& index) const;

    [[nodiscard]] nautilus::val<size_t> getNumberOfRecords() const;

    /// Returns a pointer to the wrapped TupleBuffer that is only valid for as long as this buffer is alive.
    /// It is solely intended to be passed as an argument to a `nautilus::invoke` and MUST NOT be stored or used to outlive this buffer.
    /// The lvalue-ref qualifier prevents calling it on a temporary, which would return a dangling pointer into the destroyed buffer.
    [[nodiscard]] nautilus::val<const TupleBuffer*> asArg() const&;
    nautilus::val<TupleBuffer*> asArg() &;
    /// Do not call asArg() on a temporary NautilusBuffer: the returned pointer would dangle.
    [[nodiscard]] nautilus::val<const TupleBuffer*> asArg() const&& = delete;
    nautilus::val<TupleBuffer*> asArg() && = delete;

    /// returns true if the underlying buffer is a owned buffer. Notice, that this is a c++ bool, as this information is not runtime data
    /// dependent.
    [[nodiscard]] bool isOwned() const;

    /// A NautilusBuffer is constructed either from an OwnedNautilusBuffer (empty-initialized on the stack and then assigned inside a
    /// `nautilus::invoke`) or from a BorrowedNautilusBuffer obtained via BorrowedNautilusBuffer::from. These conversions are intentionally
    /// implicit so that owned and borrowed buffers can be passed to NautilusBuffer-taking APIs ergonomically, without an explicit wrap.
    /// NOLINTBEGIN(google-explicit-constructor, hicpp-explicit-conversions)
    NautilusBuffer(OwnedNautilusBuffer);
    NautilusBuffer(BorrowedNautilusBuffer);
    /// NOLINTEND(google-explicit-constructor, hicpp-explicit-conversions)
};
}
