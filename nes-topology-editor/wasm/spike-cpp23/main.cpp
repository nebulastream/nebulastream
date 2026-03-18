#include <concepts>
#include <expected>
#include <ranges>
#include <string>
#include <vector>
#include <cstdio>

// Test std::expected (heavily used in NES StatementHandler)
std::expected<int, std::string> divide(int a, int b) {
    if (b == 0) return std::unexpected("division by zero");
    return a / b;
}

// Test concepts (used in NES templates)
template<std::integral T>
T double_it(T val) { return val * 2; }

// Test ranges (used in NES StatementHandler)
int sum_even(const std::vector<int>& v) {
    auto result = v | std::views::filter([](int x) { return x % 2 == 0; })
                    | std::views::transform([](int x) { return x; });
    int sum = 0;
    for (int x : result) sum += x;
    return sum;
}

int main() {
    // std::expected
    auto ok = divide(10, 3);
    auto err = divide(10, 0);
    if (!ok.has_value() || ok.value() != 3) return 1;
    if (err.has_value() || err.error() != "division by zero") return 2;

    // concepts
    if (double_it(21) != 42) return 3;

    // ranges
    std::vector<int> nums = {1, 2, 3, 4, 5, 6};
    if (sum_even(nums) != 12) return 4;

    std::printf("ALL C++23 TESTS PASSED\n");
    return 0;
}
