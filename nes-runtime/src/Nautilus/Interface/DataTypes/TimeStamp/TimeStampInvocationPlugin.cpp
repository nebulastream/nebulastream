/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
#include <Nautilus/Interface/DataTypes/TimeStamp/TimeStamp.hpp>
#include <Nautilus/Interface/DataTypes/InvocationPlugin.hpp>

namespace NES::Nautilus {

class TimeStampInvocationPlugin : public InvocationPlugin {
  public:
    std::optional<Value<>> Add(const Value<>& left, const Value<>& right) const override {
        if (isa<TimeStamp>(left.value) && isa<TimeStamp>(right.value)) {
            auto& ct1 = left.getValue().staticCast<TimeStamp>();
            auto& ct2 = right.getValue().staticCast<TimeStamp>();
            return Value(ct1.add(ct2));
        } else {
            return std::nullopt;
        }
    }
};

[[maybe_unused]] static InvocationPluginRegistry::Add<TimeStampInvocationPlugin> cPlugin;

}