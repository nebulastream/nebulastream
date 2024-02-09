#!/bin/bash

if ! [ -f "/build_dir/CMakeLists.txt" ]; then
	echo "Please mount POCL source code at /build_dir."
	echo "Run: docker -v /path/to/pocl-sources:/build_dir pocl_build."
fi

if [ $# -eq 0 ]; then
	cmake --fresh -B /build_dir -DCMAKE_INSTALL_PREFIX=/usr/local/pocl/ -DENABLE_CUDA=ON /pocl
	cd /build_dir
	make -j 
	make install 
	mkdir -p /etc/OpenCL/vendors 
	echo "/usr/local/pocl/lib/libpocl.so" > /etc/OpenCL/vendors/pocl.icd 
	cpack
else
	exec $@
fi
