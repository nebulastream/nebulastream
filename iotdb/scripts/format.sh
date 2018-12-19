CWDIR="$(pwd)"
BASEDIR="$(dirname "$0")"

cd ${BASEDIR}
./clang-format-all.sh ../include* ../src* ../test* && clang-format -i ../start.cpp
echo "Auto formatting is done!"
cd ${CWDIR}
