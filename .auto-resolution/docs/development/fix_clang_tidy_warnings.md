# General
As part of our CI, we are running a clang-tidy check on the codebase and the fixes are exported and provided via annotations.
This performs some static code analysis to ease of the load of the code reviewer.
We will provide the commands, assuming one uses our provided docker image.
For building it locally with vcpkg, the commands should be quite similar.
This document provides a good starting point for fixing clang-tidy warnings.
As every setup is different, it might be necessary to adjust the commands to your setup.

# Fixing clang-tidy warnings for a PR / compared to a branch
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
