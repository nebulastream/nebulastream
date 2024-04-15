//
// Created by ls on 5/24/24.
//

#ifndef RESULT_HPP
#define RESULT_HPP
#include <utility>
#include <variant>

template<typename R, typename E>
class Result {
  public:
    Result(R r) : value(r) {}
    Result(E e) : value(e) {}

    static Result Error(E e) {
        Result r{};
        r.value = {std::in_place_index<1>, e};
        return r;
    }

    R& operator*() {
        NES_ASSERT(has_value(), "Accessing Error Result");
        return std::get<0>(value);
    }

    const R& operator*() const {
        NES_ASSERT(has_value(), "Accessing Error Result");
        return std::get<0>(value);
    }

    [[nodiscard]] E& as_failure() {
        NES_ASSERT(has_error(), "Accessing Result as Error");
        return std::get<1>(value);
    }

    [[nodiscard]] const E& as_failure() const {
        NES_ASSERT(has_error(), "Accessing Result as Error");
        return std::get<1>(value);
    }

    [[nodiscard]] bool has_value() const { return value.index() == 0; }
    [[nodiscard]] bool has_error() const { return value.index() == 1; }

  private:
    std::variant<R, E> value;
};

#endif//RESULT_HPP
