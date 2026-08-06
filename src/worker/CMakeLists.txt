cmake_minimum_required(VERSION 3.20)
project(dte_worker CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-Wall -Wextra -Wpedantic)

find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPCPP   REQUIRED grpc++)
pkg_check_modules(PROTOBUF REQUIRED protobuf)
find_package(Threads REQUIRED)
find_package(nlohmann_json REQUIRED)

find_program(PROTOC          protoc          REQUIRED)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

set(GEN_DIR   ${CMAKE_CURRENT_BINARY_DIR}/generated)
set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../Contracts)
set(PROTO_SRC ${PROTO_DIR}/dte.proto)
file(MAKE_DIRECTORY ${GEN_DIR})

add_custom_command(
  OUTPUT
    ${GEN_DIR}/dte.pb.cc      ${GEN_DIR}/dte.pb.h
    ${GEN_DIR}/dte.grpc.pb.cc ${GEN_DIR}/dte.grpc.pb.h
  COMMAND ${PROTOC}
    --cpp_out=${GEN_DIR}
    --grpc_out=${GEN_DIR}
    --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
    -I ${PROTO_DIR}
    ${PROTO_SRC}
  DEPENDS ${PROTO_SRC}
  COMMENT "protoc dte.proto"
)

add_executable(dte_worker
  src/main.cpp
  src/orchestrator_client.cpp
  src/executor_pool.cpp
  src/job_registry.cpp
  src/jobs/graph_bfs.cpp
  src/jobs/noop.cpp
  ${GEN_DIR}/dte.pb.cc
  ${GEN_DIR}/dte.grpc.pb.cc
)

target_include_directories(dte_worker PRIVATE
  src
  ${GEN_DIR}
  ${GRPCPP_INCLUDE_DIRS}
  ${PROTOBUF_INCLUDE_DIRS}
)

target_link_libraries(dte_worker PRIVATE
  ${GRPCPP_LIBRARIES}
  ${PROTOBUF_LIBRARIES}
  nlohmann_json::nlohmann_json
  Threads::Threads
)
