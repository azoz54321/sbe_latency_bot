cmake_minimum_required(VERSION 3.16)
project(bsbe_native LANGUAGES CXX)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_library(bsbe_native STATIC
  src/bsbe_bridge.cc
)

target_include_directories(bsbe_native PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/generated
)

target_compile_features(bsbe_native PUBLIC cxx_std_17)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

if(MSVC)
  target_compile_options(bsbe_native PRIVATE /W4 /wd4245 /wd4244 /EHsc)
else()
  target_compile_options(bsbe_native PRIVATE -Wall -Wextra -Wpedantic)
endif()

install(TARGETS bsbe_native
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/
  DESTINATION include
  FILES_MATCHING PATTERN "*.h"
)
