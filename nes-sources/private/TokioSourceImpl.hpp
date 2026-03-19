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

#include <Sources/TokioSource.hpp>
#include <nes-io-bindings/lib.h>

namespace NES
{

/// PIMPL wrapper to hide rust::Box<SourceHandle> from the public header.
/// This avoids exposing CXX types (rust/cxx.h) in the nes-sources public API.
struct TokioSource::RustHandleImpl
{
    rust::Box<::SourceHandle> handle;

    explicit RustHandleImpl(rust::Box<::SourceHandle> h)
        : handle(std::move(h)) {}
};

} // namespace NES
