# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Functions that create the scenarios for a plugin's ``conn-test/scenarios.py``.

There is one module per kind of connector (so far only ``source``), and a plugin imports all of its scenarios from its kind's module.
Each module offers only the scenarios that its kind can bind, so a connector that imports from its kind's module cannot bind another kind's scenario.
Each constructor takes only the arguments that its scenario accepts, so an invalid argument is a ``TypeError`` when it is written,
not a defect found at run time.
"""
