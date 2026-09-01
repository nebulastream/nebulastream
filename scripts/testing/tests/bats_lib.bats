# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Guards the docker identity distributed_bats_lib.bash derives per compose
# suite: two suites sharing one delete each other's running stack. Pure shell,
# no docker daemon needed.

source "$NES_BATS_LIB"

setup_file() {
  nes_require_env NES_DIR
  nes_require_env NES_CLI
}

# Print `<label> <worker-prefix> <app-prefix> <app-image-var>` for every
# nes_distributed_setup_file call registered in the repo.
registered_identities() {
  local binref var suite
  grep -rhoE 'nes_distributed_setup_file "\$[A-Z_]+"( [a-z0-9-]+)?' \
    --include='*.bats' "$NES_DIR"/nes-* | while read -r _ binref suite; do
    var="${binref#\"\$}"
    var="${var%\"}"
    ( nes_derive_image_names "${!var}" "$suite"
      echo "$NES_BATS_TEST_LABEL $NES_BATS_WORKER_PREFIX $NES_BATS_APP_PREFIX $NES_BATS_APP_IMAGE_VAR" )
  done
}

@test "suite name defaults to the binary name without the nes- prefix" {
  nes_derive_image_names /some/where/nes-repl
  assert_equal "$NES_BATS_TEST_LABEL" distributed-repl
  assert_equal "$NES_BATS_WORKER_PREFIX" nes-worker-repl-test
  assert_equal "$NES_BATS_APP_PREFIX" nes-repl-image
  assert_equal "$NES_BATS_APP_IMAGE_VAR" REPL_IMAGE
}

@test "suites sharing a binary get distinct labels and image prefixes" {
  nes_derive_image_names /some/where/nes-cli mqtt-sink
  assert_equal "$NES_BATS_TEST_LABEL" distributed-mqtt-sink
  assert_equal "$NES_BATS_WORKER_PREFIX" nes-worker-mqtt-sink-test
  assert_equal "$NES_BATS_APP_PREFIX" nes-mqtt-sink-image
  # The compose files reference the app image by binary, not by suite.
  assert_equal "$NES_BATS_APP_IMAGE_VAR" CLI_IMAGE
}

@test "no two registered suites share a docker identity" {
  local identities count unique
  identities=$(registered_identities)
  [ -n "$identities" ] || fail "found no nes_distributed_setup_file callers"

  # Any single repeated field is a collision: cleanup matches networks by label
  # and images by `<prefix>-*` independently.
  local field
  for field in 1 2 3; do
    count=$(echo "$identities" | cut -d' ' -f$field | wc -l)
    unique=$(echo "$identities" | cut -d' ' -f$field | sort -u | wc -l)
    if [ "$count" -ne "$unique" ]; then
      echo "duplicate identity field $field across suites:" >&2
      echo "$identities" >&2
      fail "each distributed suite needs its own suite name (2nd arg to nes_distributed_setup_file)"
    fi
  done
}

@test "no two compose files declare the same nes-test network label" {
  local labels count unique
  # Strips `${NES_BATS_TEST_LABEL:-<default>}` down to the default, which a
  # suite falls back to and so has to be unique too.
  labels=$(grep -rh 'nes-test:' --include='create_compose.sh' "$NES_DIR"/nes-* \
    | sed -e 's/.*nes-test: *//' -e 's/^\${NES_BATS_TEST_LABEL:-//' -e 's/}$//')
  [ -n "$labels" ] || fail "found no nes-test network labels"

  count=$(echo "$labels" | wc -l)
  unique=$(echo "$labels" | sort -u | wc -l)
  if [ "$count" -ne "$unique" ]; then
    echo "duplicate nes-test labels:" >&2
    echo "$labels" | sort >&2
    fail "compose files must not share a network label"
  fi
}
