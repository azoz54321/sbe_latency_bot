cmake_minimum_required(VERSION 3.16)
project(native_bsbe LANGUAGES CXX)

# نبني مكتبة الجسر
add_library(bsbe_native STATIC src/bsbe_bridge.cc)

# نضيف مجلد الهيدرز المولّدة: native/include/spot_sbe/...
target_include_directories(bsbe_native PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

# إعدادات C++
set_target_properties(bsbe_native PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

# تثبيت (اختياري)
install(TARGETS bsbe_native DESTINATION lib)
