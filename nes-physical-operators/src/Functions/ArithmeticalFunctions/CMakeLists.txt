# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

register_entry(Abs PhysicalFunction AbsolutePhysicalFunction.cpp)
register_entry(Add PhysicalFunction AddPhysicalFunction.cpp)
register_entry(Ceil PhysicalFunction CeilPhysicalFunction.cpp)
register_entry(Div PhysicalFunction DivPhysicalFunction.cpp)
register_entry(Floor PhysicalFunction FloorPhysicalFunction.cpp)
register_entry(Mod PhysicalFunction ModPhysicalFunction.cpp)
register_entry(Mul PhysicalFunction MulPhysicalFunction.cpp)
register_entry(Pow PhysicalFunction PowPhysicalFunction.cpp)
register_entry(Round PhysicalFunction RoundPhysicalFunction.cpp)
register_entry(Sub PhysicalFunction SubPhysicalFunction.cpp)
register_entry(Sqrt PhysicalFunction SqrtPhysicalFunction.cpp)
