# Vendored Corrosion

This directory contains a vendored snapshot of [Corrosion](https://github.com/corrosion-rs/corrosion),
the CMake/Rust integration used by NebulaStream to build the `nes-network` Rust bindings
(see `cmake/EnableRust.cmake`).

Corrosion is vendored in-tree instead of fetched over the network so that fresh configures
(including fully offline builds inside the dev Docker image) do not depend on GitHub
reachability.

## Origin

- **Upstream:** https://github.com/corrosion-rs/corrosion
- **Fork we track:** https://github.com/nebulastream/corrosion.git
- **Ref:** `v0.6.1-always-dirty-fix` (branch `fix/avoid-always-dirty-targets`)
- **Vendored commit:** `5ff20ed3b025d2170617d9844073cc7221439877`

## Patch carried by the fork

The fork carries a small patch on top of upstream Corrosion that avoids Rust targets being
treated as *always dirty*, which otherwise forced a full Rust rebuild on every CMake build
even when no Rust sources changed. Everything else matches upstream Corrosion `v0.6.1`.

## How to bump the vendored copy

To update to a newer fork ref:

```shell
# 1. Clone the desired ref (tag or branch) of the fork, without history.
git clone --depth 1 --branch <new-ref> https://github.com/nebulastream/corrosion.git /tmp/corrosion-snapshot

# 2. Record the commit you snapshotted (paste it into the "Vendored commit" field above).
git -C /tmp/corrosion-snapshot rev-parse HEAD

# 3. Strip the git metadata so it becomes a plain vendored snapshot.
rm -rf /tmp/corrosion-snapshot/.git

# 4. Replace the vendored tree (keep this VENDORING.md).
cp cmake/corrosion/VENDORING.md /tmp/VENDORING.md.bak
rm -rf cmake/corrosion
mkdir -p cmake/corrosion
cp -R /tmp/corrosion-snapshot/. cmake/corrosion/
mv /tmp/VENDORING.md.bak cmake/corrosion/VENDORING.md

# 5. Update the Ref / Vendored commit fields above, then commit.
git add -A cmake/corrosion
```

Because the snapshot now lives in-tree, the hash of the Rust dependency Docker image is
derived from the normal source hashing of these files; there is no separate moving ref to
track in `docker/dependency/hash_dependencies.sh`.
