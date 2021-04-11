#ifndef NES_TESTS_UTIL_NESTEST_HPP_
#define NES_TESTS_UTIL_NESTEST_HPP_
#include <gtest/gtest.h>
#include <typeinfo>
#define ASSERT_INSTANCE_OF(node, instance)                                                                                       \
    if (!node->instanceOf<instance>()) {                                                                                         \
        auto message = node->toString() + " is not of instance " + std::string(typeid(instance).name());                                                     \
        GTEST_FATAL_FAILURE_(message.c_str());      \
    }
#endif//NES_TESTS_UTIL_NESTEST_HPP_
