#include "buffer.hh"

#include <cstring>

#include "errors.hh"

using namespace dibibase::util;

MemoryBuffer::MemoryBuffer(size_t size)
    : m_size(size), m_offset(0),
      m_buf(std::make_unique<unsigned char[]>(m_size)) {}

MemoryBuffer::MemoryBuffer(std::unique_ptr<unsigned char[]> buf, size_t size)
    : m_size(size), m_offset(0), m_buf(std::move(buf)) {}

uint64_t MemoryBuffer::get_uint64() {
  uint64_t data = 0;
  constexpr auto size = sizeof(uint64_t);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(reinterpret_cast<unsigned char *>(&data), m_buf.get() + m_offset,
         size);
  advance(size);
  return data;
}

Buffer &MemoryBuffer::put_uint64(uint64_t data) {
  constexpr auto size = sizeof(uint64_t);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(m_buf.get() + m_offset, reinterpret_cast<unsigned char *>(&data),
         size);
  advance(size);
  return *this;
}

uint32_t MemoryBuffer::get_uint32() {
  uint32_t data = 0;
  constexpr auto size = sizeof(uint32_t);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(reinterpret_cast<unsigned char *>(&data), m_buf.get() + m_offset,
         size);
  advance(size);
  return data;
}

Buffer &MemoryBuffer::put_uint32(uint32_t data) {
  constexpr auto size = sizeof(uint32_t);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(m_buf.get() + m_offset, reinterpret_cast<unsigned char *>(&data),
         size);
  advance(size);
  return *this;
}

uint16_t MemoryBuffer::get_uint16() {
  uint16_t data = 0;
  constexpr auto size = sizeof(uint16_t);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(reinterpret_cast<unsigned char *>(&data), m_buf.get() + m_offset,
         size);
  advance(size);
  return data;
}

Buffer &MemoryBuffer::put_uint16(uint16_t data) {
  constexpr auto size = sizeof(uint16_t);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(m_buf.get() + m_offset, reinterpret_cast<unsigned char *>(&data),
         size);
  advance(size);
  return *this;
}

uint8_t MemoryBuffer::get_uint8() {
  uint8_t data = 0;
  constexpr auto size = sizeof(uint8_t);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(reinterpret_cast<unsigned char *>(&data), m_buf.get() + m_offset,
         size);
  advance(size);
  return data;
}

Buffer &MemoryBuffer::put_uint8(uint8_t data) {
  constexpr auto size = sizeof(uint8_t);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(m_buf.get() + m_offset, reinterpret_cast<unsigned char *>(&data),
         size);
  advance(size);
  return *this;
}

int64_t MemoryBuffer::get_int64() {
  int64_t data = 0;
  constexpr auto size = sizeof(int64_t);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(reinterpret_cast<unsigned char *>(&data), m_buf.get() + m_offset,
         size);
  advance(size);
  return data;
}

Buffer &MemoryBuffer::put_int64(int64_t data) {
  constexpr auto size = sizeof(int64_t);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(m_buf.get() + m_offset, reinterpret_cast<unsigned char *>(&data),
         size);
  advance(size);
  return *this;
}

int32_t MemoryBuffer::get_int32() {
  int32_t data = 0;
  constexpr auto size = sizeof(int32_t);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(reinterpret_cast<unsigned char *>(&data), m_buf.get() + m_offset,
         size);
  advance(size);
  return data;
}

Buffer &MemoryBuffer::put_int32(int32_t data) {
  constexpr auto size = sizeof(int32_t);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(m_buf.get() + m_offset, reinterpret_cast<unsigned char *>(&data),
         size);
  advance(size);
  return *this;
}

int16_t MemoryBuffer::get_int16() {
  int16_t data = 0;
  constexpr auto size = sizeof(int16_t);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(reinterpret_cast<unsigned char *>(&data), m_buf.get() + m_offset,
         size);
  advance(size);
  return data;
}

Buffer &MemoryBuffer::put_int16(int16_t data) {
  constexpr auto size = sizeof(int16_t);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(m_buf.get() + m_offset, reinterpret_cast<unsigned char *>(&data),
         size);
  advance(size);
  return *this;
}

int8_t MemoryBuffer::get_int8() {
  int8_t data = 0;
  constexpr auto size = sizeof(int8_t);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(reinterpret_cast<unsigned char *>(&data), m_buf.get() + m_offset,
         size);
  advance(size);
  return data;
}

Buffer &MemoryBuffer::put_int8(int8_t data) {
  constexpr auto size = sizeof(int8_t);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(m_buf.get() + m_offset, reinterpret_cast<unsigned char *>(&data),
         size);
  advance(size);
  return *this;
}

std::string MemoryBuffer::get_string(int size) {
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  std::string data;
  for (int i = m_offset; i < (int)(size + m_offset); ++i) {
    data.push_back((char)m_buf[i]);
  }
  advance(size);
  return data;
}

Buffer &MemoryBuffer::put_string(std::string data) {
  int size = data.size();
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(m_buf.get() + m_offset, data.c_str(), size);
  advance(size);
  return *this;
}

double MemoryBuffer::get_double() {
  double data = 0;
  constexpr auto size = sizeof(double);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(reinterpret_cast<unsigned char *>(&data), m_buf.get() + m_offset,
         size);
  advance(size);
  return data;
}

Buffer &MemoryBuffer::put_double(double data) {
  constexpr auto size = sizeof(double);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(m_buf.get() + m_offset, reinterpret_cast<unsigned char *>(&data),
         size);
  advance(size);
  return *this;
}

float MemoryBuffer::get_float() {
  float data = 0;
  constexpr auto size = sizeof(float);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(reinterpret_cast<unsigned char *>(&data), m_buf.get() + m_offset,
         size);
  advance(size);
  return data;
}

Buffer &MemoryBuffer::put_float(float data) {
  constexpr auto size = sizeof(float);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(m_buf.get() + m_offset, reinterpret_cast<unsigned char *>(&data),
         size);
  advance(size);
  return *this;
}

bool MemoryBuffer::get_boolean() {
  bool data = 0;
  constexpr auto size = sizeof(bool);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(reinterpret_cast<unsigned char *>(&data), m_buf.get() + m_offset,
         size);
  advance(size);
  return data;
}

Buffer &MemoryBuffer::put_boolean(bool data) {
  constexpr auto size = sizeof(bool);
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(m_buf.get() + m_offset, reinterpret_cast<unsigned char *>(&data),
         size);
  advance(size);
  return *this;
}

std::unique_ptr<unsigned char[]> MemoryBuffer::get_blob(int size) {
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  std::unique_ptr<unsigned char[]> data =
      std::make_unique<unsigned char[]>(size);
  memcpy(data.get(), m_buf.get() + m_offset, size);
  advance(size);
  return data;
}

Buffer &MemoryBuffer::put_blob(unsigned char *data, int size) {
  if (is_overflow(size)) {
    throw "Buffer Overflow";
  }
  memcpy(m_buf.get() + m_offset, data, size);
  advance(size);
  return *this;
}

const std::unique_ptr<unsigned char[]> MemoryBuffer::bytes() const {
  std::unique_ptr<unsigned char[]> buf =
      std::make_unique<unsigned char[]>(m_size);

  // Copying buffer content.
  for (int i = 0; i < (int)m_size; ++i) {
    buf[i] = m_buf[i];
  }
  return buf;
}
