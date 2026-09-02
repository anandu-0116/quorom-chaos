cmake_minimum_required(VERSION 3.16)
project(quorum_chaos CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

# Homebrew installs gRPC/Protobuf outside the default search path on Apple
# Silicon; point CMake at it if it's not already on CMAKE_PREFIX_PATH.
if(EXISTS "/opt/homebrew/opt/grpc")
  list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew/opt/grpc" "/opt/homebrew/opt/protobuf")
endif()

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# --- Generate C++ message + gRPC service code from proto/raft.proto ---
set(PROTO_FILE ${CMAKE_SOURCE_DIR}/proto/raft.proto)
set(GENERATED_DIR ${CMAKE_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_DIR})

set(PROTO_SRCS ${GENERATED_DIR}/raft.pb.cc)
set(PROTO_HDRS ${GENERATED_DIR}/raft.pb.h)
set(GRPC_SRCS ${GENERATED_DIR}/raft.grpc.pb.cc)
set(GRPC_HDRS ${GENERATED_DIR}/raft.grpc.pb.h)

find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin
  HINTS /opt/homebrew/opt/grpc/bin)
if(NOT GRPC_CPP_PLUGIN)
  message(FATAL_ERROR "grpc_cpp_plugin not found -- install gRPC (e.g. `brew install grpc`)")
endif()

add_custom_command(
  OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
  COMMAND protobuf::protoc
  ARGS --cpp_out=${GENERATED_DIR}
       --grpc_out=${GENERATED_DIR}
       --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
       -I ${CMAKE_SOURCE_DIR}/proto
       ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
  COMMENT "Generating C++ code from raft.proto"
)

add_library(raft_proto ${PROTO_SRCS} ${GRPC_SRCS})
target_include_directories(raft_proto PUBLIC ${GENERATED_DIR})
target_link_libraries(raft_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

# --- quorum-chaos node ---
add_executable(raft_node
  src/main.cpp
  src/persistent_state.cpp
  src/election_timer.cpp
  src/raft_node.cpp
  src/config.cpp
)
target_include_directories(raft_node PRIVATE src)
target_link_libraries(raft_node PRIVATE raft_proto gRPC::grpc++)
