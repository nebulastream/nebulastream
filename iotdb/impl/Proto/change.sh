
export PATH="$PATH:/home/zeuchste/git/IoTDB/iotdb/cmake-build-debug/_deps/grpc-build"
cd /home/zeuchste/git/IoTDB/iotdb/impl/Proto
/home/zeuchste/git/IoTDB/iotdb/cmake-build-debug/_deps/grpc-build/third_party/protobuf/protoc -I /home/zeuchste/git/IoTDB/iotdb/impl/Proto/ --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` /home/zeuchste/git/IoTDB/iotdb/impl/Proto/CoordinatorRPCService.proto
/home/zeuchste/git/IoTDB/iotdb/cmake-build-debug/_deps/grpc-build/third_party/protobuf/protoc -I /home/zeuchste/git/IoTDB/iotdb/impl/Proto/  --cpp_out=. CoordinatorRPCService.proto

cp Coor*.h ../../include/GRPC
cp Coor*.cc ../GRPC/ 

find ../GRPC -type f -name "*.cc" -exec sed -i'' -e 's/#include \"CoordinatorRPCService.pb.h\"/#include <GRPC\/CoordinatorRPCService.pb.h>/g' {} +
find ../GRPC -type f -name "*.cc" -exec sed -i'' -e 's/#include \"CoordinatorRPCService.grpc.pb.h\"/#include <GRPC\/CoordinatorRPCService.grpc.pb.h>/g' {} +

