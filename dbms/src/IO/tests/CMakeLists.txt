# Copyright 2022 PingCAP, Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include_directories (${CMAKE_CURRENT_BINARY_DIR})
set(SRCS )

add_executable (read_buffer read_buffer.cpp ${SRCS})
target_link_libraries (read_buffer clickhouse_common_io)

add_executable (read_buffer_perf read_buffer_perf.cpp ${SRCS})
target_link_libraries (read_buffer_perf clickhouse_common_io)

add_executable (read_float_perf read_float_perf.cpp ${SRCS})
target_link_libraries (read_float_perf clickhouse_common_io)

add_executable (write_buffer write_buffer.cpp ${SRCS})
target_link_libraries (write_buffer clickhouse_common_io)

add_executable (write_buffer_perf write_buffer_perf.cpp ${SRCS})
target_link_libraries (write_buffer_perf clickhouse_common_io)

add_executable (valid_utf8_perf valid_utf8_perf.cpp ${SRCS})
target_link_libraries (valid_utf8_perf clickhouse_common_io)

add_executable (valid_utf8 valid_utf8.cpp ${SRCS})
target_link_libraries (valid_utf8 clickhouse_common_io)

add_executable (compressed_buffer compressed_buffer.cpp ${SRCS})
target_link_libraries (compressed_buffer clickhouse_common_io)

add_executable (var_uint var_uint.cpp ${SRCS})
target_link_libraries (var_uint clickhouse_common_io)

add_executable (read_escaped_string read_escaped_string.cpp ${SRCS})
target_link_libraries (read_escaped_string clickhouse_common_io)

add_executable (async_write async_write.cpp ${SRCS})
target_link_libraries (async_write clickhouse_common_io)

add_executable (parse_int_perf parse_int_perf.cpp ${SRCS})
target_link_libraries (parse_int_perf clickhouse_common_io)

add_executable (parse_int_perf2 parse_int_perf2.cpp ${SRCS})
target_link_libraries (parse_int_perf2 clickhouse_common_io)

add_executable (read_write_int read_write_int.cpp ${SRCS})
target_link_libraries (read_write_int clickhouse_common_io)

add_executable (mempbrk mempbrk.cpp ${SRCS})
target_link_libraries (mempbrk clickhouse_common_io)

add_executable (cached_compressed_read_buffer cached_compressed_read_buffer.cpp ${SRCS})
target_link_libraries (cached_compressed_read_buffer clickhouse_common_io)

add_executable (o_direct_and_dirty_pages o_direct_and_dirty_pages.cpp ${SRCS})
target_link_libraries (o_direct_and_dirty_pages clickhouse_common_io)

add_executable (hashing_write_buffer hashing_write_buffer.cpp ${SRCS})
target_link_libraries (hashing_write_buffer clickhouse_common_io)
add_check(hashing_write_buffer)

add_executable (hashing_read_buffer hashing_read_buffer.cpp)
target_link_libraries (hashing_read_buffer clickhouse_common_io)
add_check (hashing_read_buffer)

add_executable (io_operators operators.cpp ${SRCS})
target_link_libraries (io_operators clickhouse_common_io)

if (NOT OS_DARWIN AND NOT OS_FREEBSD)
    add_executable(write_buffer_aio write_buffer_aio.cpp)
    target_link_libraries (write_buffer_aio clickhouse_common_io ${Boost_FILESYSTEM_LIBRARY})

    add_executable(read_buffer_aio read_buffer_aio.cpp)
    target_link_libraries (read_buffer_aio clickhouse_common_io ${Boost_FILESYSTEM_LIBRARY})
endif ()

add_executable (zlib_buffers zlib_buffers.cpp ${SRCS})
target_link_libraries (zlib_buffers clickhouse_common_io)

add_executable (remote_read_write_buffer remote_read_write_buffer.cpp ${SRCS})
target_link_libraries (remote_read_write_buffer clickhouse_common_io)

add_executable (limit_read_buffer limit_read_buffer.cpp ${SRCS})
target_link_libraries (limit_read_buffer clickhouse_common_io)

add_executable (limit_read_buffer2 limit_read_buffer2.cpp ${SRCS})
target_link_libraries (limit_read_buffer2 clickhouse_common_io)

add_executable (parse_date_time_best_effort parse_date_time_best_effort.cpp ${SRCS})
target_link_libraries (parse_date_time_best_effort clickhouse_common_io)
