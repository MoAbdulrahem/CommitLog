cmake_minimum_required(VERSION 3.20.0)
set(CMAKE_BUILD_TYPE Debug)

project(Commitlog-Prototype LANGUAGES CXX)

add_executable("${PROJECT_NAME}" main.cpp )

include_directories("${PROJECT_SOURCE_DIR}")
include_directories("${PROJECT_SOURCE_DIR}/include")
include_directories("${PROJECT_SOURCE_DIR}/util")
include_directories("${PROJECT_SOURCE_DIR}/src")

set(CMAKE_CXX_FLAGS -pthread)
find_package(Threads REQUIRED)
target_link_libraries(${PROJECT_NAME} PRIVATE Threads::Threads)
