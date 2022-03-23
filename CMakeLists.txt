cmake_minimum_required(VERSION 2.4)
project(NBTree)

#set(CMAKE_CXX_FLAGS "-std=c++11 ${CMAKE_CXX_FLAGS} -DPERF_LATENCY -DUSE_NVM_MALLOC -DNO_CONCURRENT -DNDEBUG -g -O2 -mrtm")
set(CMAKE_CXX_FLAGS "-std=c++11 ${CMAKE_CXX_FLAGS}  -DUSE_NVM_MALLOC -DCLEAR_NVM_POOL -g -O2 -mrtm")

link_directories("${PROJECT_SOURCE_DIR}/third-party-lib")
link_libraries("${PROJECT_SOURCE_DIR}/third-party-lib/libtbb.so.2")
link_directories(/usr/lib/x86_64-linux-gnu/)
link_libraries(boost_system boost_thread pthread pmemobj pmem)

include_directories("third-party-lib")
include_directories("include")

add_executable(nbtree
	src/nbtree.cpp
	src/util.cpp
)

# add_executable(data_generator
# 	src/data_generator.cpp
# 	src/util.cpp
# )



