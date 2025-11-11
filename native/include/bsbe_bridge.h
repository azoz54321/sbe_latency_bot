#pragma once

#include <cstddef>
#include <cstdint>

extern "C" {

struct BsbeTrade {
  std::int64_t px_e8;
  std::int64_t qty_e8;
  std::uint8_t side;       // 0 = buy, 1 = sell
  std::uint8_t _pad0;
  std::uint16_t _pad1;
  std::uint32_t symbol_id; // optional (0 when not provided)
  std::uint64_t event_ts_ns;
  std::uint64_t trade_id;
};

int bsbe_try_decode_one(
  const std::uint8_t* buf,
  std::size_t len,
  BsbeTrade* out,
  std::size_t out_cap,
  std::size_t* consumed
);

}
