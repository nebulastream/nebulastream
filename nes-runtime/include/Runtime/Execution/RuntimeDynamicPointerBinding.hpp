#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <ErrorHandling.hpp>

namespace NES
{
class RuntimeDynamicPointerBinding final
{
public:
    template <typename OwnerType, typename PointerType>
    static RuntimeDynamicPointerBinding create(std::string name, std::shared_ptr<OwnerType> owner, PointerType* pointer)
    {
        PRECONDITION(not name.empty(), "Dynamic pointer binding name must not be empty");
        PRECONDITION(owner != nullptr, "Dynamic pointer binding owner must not be null");
        PRECONDITION(pointer != nullptr, "Dynamic pointer binding pointer must not be null");
        return RuntimeDynamicPointerBinding(std::move(name), std::shared_ptr<const void>(std::move(owner)), pointer);
    }

    [[nodiscard]] const std::string& getName() const { return name; }
    [[nodiscard]] uint64_t getPointerValue() const { return reinterpret_cast<uint64_t>(pointer); }

private:
    RuntimeDynamicPointerBinding(std::string name, std::shared_ptr<const void> owner, const void* pointer)
        : name(std::move(name)), owner(std::move(owner)), pointer(pointer)
    {
    }

    std::string name;
    std::shared_ptr<const void> owner;
    const void* pointer;
};

}
