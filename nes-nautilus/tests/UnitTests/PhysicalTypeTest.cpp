//
// Created by ls on 4/23/25.
//

#include "PhysicalType.hpp"
#include "Nautilus/DataTypes/VarVal.hpp"

#include <gtest/gtest.h>

#include <Engine.hpp>

TEST(ATEst, a)
{
    nautilus::engine::Options options;
    nautilus::engine::NautilusEngine engine(options);

    auto fn = engine.registerFunction(+[](nautilus::val<int8_t> a, nautilus::val<int8_t> b)
                                      {
                                          Integer i{};
                                          Integer d{};
                                          i.load({a});
                                          d.load({b});
                                          auto n = Add(i, d);
                                          return expect_s<int8_t>(n.store()[0]);
                                      });

    EXPECT_EQ(fn(8, 12), 20);
}