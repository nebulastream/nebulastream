# General
As part of our CI, we are running a clang-tidy check on the codebase and the fixes are exported and provided via annotations.
This performs some static code analysis to ease of the load of the code reviewer.
We will provide the commands, assuming one uses our provided docker image.
For building it locally with vcpkg, the commands should be quite similar.
This document provides a good starting point for fixing clang-tidy warnings.
As every setup is different, it might be necessary to adjust the commands to your setup.

# Running clang-tidy via CMake targets (recommended)
Just like `clang-format` is available as the `format` / `check-format` CMake targets, clang-tidy on your diff is
available as CMake targets. These wrap `clang-tidy-diff.py` so you no longer need the hand-written `git diff | clang-tidy-diff-19.py ...`
command below. There are four targets:

| Target | Diff base | Mode |
| --- | --- | --- |
| `tidy-diff` | `NES_TIDY_DIFF_BASE` (default `HEAD`, i.e. all uncommitted changes) | check |
| `tidy-diff-fix` | same as above | applies `-fix` |
| `tidy-diff-to-main` | `origin/main` (whole branch) | check |
| `tidy-diff-to-main-fix` | `origin/main` (whole branch) | applies `-fix` |

For reviewing a PR, the usual command is to fix everything on your branch relative to `origin/main`:
```bash
cmake --build build --target tidy-diff-to-main-fix
```
Or, wrapped in the development container (the `-to-main` targets pin the base internally, so no `-e NES_TIDY_DIFF_BASE=...`
plumbing is needed):
```bash
docker run \
    --workdir $(pwd) \
    -v $(pwd):$(pwd) \
    nebulastream/nes-development:local \
    cmake --build build-docker --target tidy-diff-to-main-fix
```
In CLion these appear in the target dropdown, so you can run them like any other build target — no need to edit the
Docker toolchain environment.

Notes:
- **Build first.** Some headers (gRPC/protobuf stubs, config headers) are generated during the build; without them
  clang-tidy reports spurious `file not found` errors. The targets print a hint when this happens.
- The targets are only registered when `CMAKE_EXPORT_COMPILE_COMMANDS=ON` (on by default) — clang-tidy-diff needs the
  `compile_commands.json`.
- To compare against an arbitrary base (a branch or commit other than `origin/main`), set `NES_TIDY_DIFF_BASE` and use
  the plain `tidy-diff` / `tidy-diff-fix` targets, e.g. `NES_TIDY_DIFF_BASE=origin/some-branch cmake --build build --target tidy-diff-fix`.

# Running the clang-tidy diff workflow with Nix
If you want to reproduce the current clang-tidy diff workflow locally with the Nix toolchain, run the following command
from the repository root.
```bash
nix run .#clang-tidy
```
By default, the command compares the current checkout to `origin/main`.
To use another branch or commit, pass it after `--`, for example `nix run .#clang-tidy -- upstream/main`.
The command uses the official `clang-tidy-diff.py` workflow, applies fixes in place, and configures and builds
`build/`.

# Manual invocation (fallback / custom setups)
The CMake targets above are preferred. Use the manual commands below only for custom bases or non-CMake setups.

As a pre-requisite, you need to have the docker image built and your git repository updated.
Before running clang-tidy, we must create a running container from the image.
If possible, you should run the Docker container in rootless mode. 
Otherwise, the clang-tidy check will change the ownership of the files to root, and you will have to change it back. 
```bash
docker run --rm -it -v <path/to/nebulastream>:/tmp/nebulastream nebulastream/nes-development
```

Then, we can run the following command to fix the clang-tidy warnings inside the Docker container.
Before running the command, please change the `<no. threads>`.
If you want run clang-tidy on the diff to another branch, please change `origin/main` to that branch.
The below command assumes that NebulaStream is mounted under `/tmp/nebulastream` in the docker image.
We exclude '*.inc' files, since '*.inc' files are dependent header files that other header files include and that therefore don't need to compile on their own.
```bash
export LLVM_SYMBOLIZER_PATH=llvm-symbolizer-19 && \
    git config --global --add safe.directory /tmp/nebulastream && \
    cd /tmp/nebulastream && \
    rm -rf build/ && mkdir build && \
    cmake -GNinja -B build -DCMAKE_EXPORT_COMPILE_COMMANDS=ON && \
    git diff -U0 origin/main -- ':!*.inc' | clang-tidy-diff-19.py -clang-tidy-binary clang-tidy-19 -p1 -path build -fix -config-file .clang-tidy -use-color -j <no. threads>
```
Since we generate some header files in the build process, clang-tidy might complain about missing header files.
In this case, you have to build `NebulaStream` before running the clang-tidy check to create the missing header files.
```bash
export LLVM_SYMBOLIZER_PATH=llvm-symbolizer-19 && \
    git config --global --add safe.directory /tmp/nebulastream && \
    cd /tmp/nebulastream && \
    rm -rf build/ && mkdir build && \
    cmake -GNinja -B build -DCMAKE_EXPORT_COMPILE_COMMANDS=ON && \
    cmake --build build -j -- -k 0 && \
    git diff -U0 origin/main -- ':!*.inc' | clang-tidy-diff-19.py -clang-tidy-binary clang-tidy-19 -p1 -path build -fix -config-file .clang-tidy -use-color -j <no. threads>
```

# Fixing clang-tidy warnings compared to a commit hash
If you want to fix the clang-tidy warnings compared to a hash commit, the commands are quite similar.
The only difference is that you replace the `<branch name>` with the hash commit.
```bash
git diff -U0 origin/<branch name> -- ':!*.inc'
git diff -U0 ${START_COMMIT_SHA} -- ':!*.inc'
```

# Important notes
There are some important notes to consider when running clang-tidy to fix the warnings.
- It might take a while to run the clang-tidy check, depending on the number of files and the number of threads you use.
- You should not do anything to the codebase while the clang-tidy check is running. No switching branches, no rebasing, no committing, no editing files, etc. Grab yourself a coffee and wait for the clang-tidy check to finish.
- It might happen that clang-tidy will run into compiler errors. In this case, you have to fix the compiler errors first before running the clang-tidy check again.


## FAQ
It might happen that you cannot edit the `NebulaStream directory, after running the clang-tidy check.
This might be due to the fact that the folder is now owned by root, as the Docker container runs as root (if you use the provided docker command and don't use rootless mode).
To fix this, you can run the following command.
```bash
sudo chown -R $USER:$USER <path/to/nebulastream>
```
