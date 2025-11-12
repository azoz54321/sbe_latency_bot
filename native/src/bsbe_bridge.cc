#include "bsbe_bridge.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>

#include "spot_sbe/MessageHeader.h"
#include "spot_sbe/TradesResponse.h"
#include "spot_sbe/WebSocketResponse.h"

// ---- SBE Trade event includes across spot_stream schema variants ----
#if __has_include("spot_stream/TradeEvent.h")
#  include "spot_stream/TradeEvent.h"
#elif __has_include("spot_stream/TradesStreamEvent.h")
#  include "spot_stream/TradesStreamEvent.h"
#elif __has_include("spot_stream/Trades.h")
#  include "spot_stream/Trades.h"
#else
#  error "No known Trade* header found under spot_stream."
#endif

#if __has_include("spot_stream/BoolEnum.h")
#  include "spot_stream/BoolEnum.h"
#else
#  error "BoolEnum.h not found under spot_stream."
#endif

namespace {

constexpr std::size_t kHeaderSize = 8;
constexpr std::uint16_t kSpotSchemaId = 3;
constexpr std::uint16_t kStreamSchemaId = 1;
constexpr std::uint16_t kTradesTemplateId = 201;
constexpr std::uint16_t kWsTemplateId = 50;
constexpr std::uint16_t kStreamTradesTemplateId =
    spot_stream::TradesStreamEvent::SBE_TEMPLATE_ID;

constexpr std::int32_t kErrIncomplete = -1;
constexpr std::int32_t kErrCorrupt = -2;
constexpr std::int32_t kErrSchemaMismatch = -3;
constexpr std::int32_t kErrOutputTruncated = -4;

constexpr std::int64_t kPow10[] = {
    1LL,
    10LL,
    100LL,
    1'000LL,
    10'000LL,
    100'000LL,
    1'000'000LL,
    10'000'000LL,
    100'000'000LL,
    1'000'000'000LL,
    10'000'000'000LL,
    100'000'000'000LL,
    1'000'000'000'000LL,
    10'000'000'000'000LL,
    100'000'000'000'000LL,
    1'000'000'000'000'000LL,
    10'000'000'000'000'000LL,
    100'000'000'000'000'000LL,
};

constexpr std::size_t kPow10Size = sizeof(kPow10) / sizeof(kPow10[0]);

std::int64_t clamp_mul(std::int64_t value, std::int64_t factor) {
  if (factor == 1) {
    return value;
  }
  if (factor == 0 || value == 0) {
    return 0;
  }
  if (value > 0) {
    if (factor > 0 &&
        value > std::numeric_limits<std::int64_t>::max() / factor) {
      return std::numeric_limits<std::int64_t>::max();
    }
    if (factor < 0 &&
        value > std::numeric_limits<std::int64_t>::min() / factor) {
      return std::numeric_limits<std::int64_t>::min();
    }
  } else {
    if (factor > 0 &&
        value < std::numeric_limits<std::int64_t>::min() / factor) {
      return std::numeric_limits<std::int64_t>::min();
    }
    if (factor < 0 &&
        value < std::numeric_limits<std::int64_t>::max() / factor) {
      return std::numeric_limits<std::int64_t>::max();
    }
  }
  return value * factor;
}

std::int64_t scale_to_e8(std::int64_t mantissa, std::int32_t exponent) {
  const std::int32_t shift = exponent + 8;
  if (shift >= 0) {
    if (static_cast<std::size_t>(shift) >= kPow10Size) {
      return mantissa >= 0 ? std::numeric_limits<std::int64_t>::max()
                           : std::numeric_limits<std::int64_t>::min();
    }
    return clamp_mul(mantissa, kPow10[shift]);
  }

  std::int32_t down = -shift;
  while (down-- > 0) {
    mantissa /= 10;
  }
  return mantissa;
}

std::uint64_t to_ns(std::uint64_t time_us) {
  if (time_us >
      std::numeric_limits<std::uint64_t>::max() / static_cast<std::uint64_t>(1000)) {
    return std::numeric_limits<std::uint64_t>::max();
  }
  return time_us * static_cast<std::uint64_t>(1000);
}

std::uint64_t to_ns_signed(std::int64_t time_us) {
  if (time_us <= 0) {
    return 0;
  }
  return to_ns(static_cast<std::uint64_t>(time_us));
}

std::uint8_t to_side(spot_sbe::BoolEnum::Value maker_flag) {
  return maker_flag == spot_sbe::BoolEnum::True ? 1 : 0;
}

int decode_trades_payload(char* payload,
                          std::size_t payload_len,
                          std::uint16_t acting_block_length,
                          std::uint16_t acting_version,
                          BsbeTrade* out,
                          std::size_t out_cap,
                          std::size_t* consumed) {
  if (out_cap == 0) {
    return kErrOutputTruncated;
  }

  spot_sbe::TradesResponse trades;
  try {
    trades.wrapForDecode(
        payload, kHeaderSize, acting_block_length, acting_version, payload_len);
  } catch (const std::exception&) {
    return kErrCorrupt;
  }

  const std::int32_t price_exp = trades.priceExponent();
  const std::int32_t qty_exp = trades.qtyExponent();

  auto& group = trades.trades();
  std::size_t index = 0;
  while (group.hasNext()) {
    group.next();
    if (index >= out_cap) {
      try {
        // exhaust the iterator so sbePosition() is correct
        while (group.hasNext()) {
          group.next();
        }
      } catch (...) {
        // ignore secondary errors
      }
      *consumed = trades.sbePosition();
      return kErrOutputTruncated;
    }

    BsbeTrade& dst = out[index];
    const std::int64_t raw_price = group.price();
    const std::int64_t raw_qty = group.qty();
    dst.px_e8 = scale_to_e8(raw_price, price_exp);
    dst.qty_e8 = scale_to_e8(raw_qty, qty_exp);
    dst.side = to_side(group.isBuyerMaker());
    dst._pad0 = 0;
    dst._pad1 = 0;
    dst.symbol_id = 0;
    dst.event_ts_ns = to_ns(group.time());
    dst.trade_id = static_cast<std::uint64_t>(group.id());
    ++index;
  }

  *consumed = trades.sbePosition();
  return static_cast<int>(index);
}

int decode_stream_trades_message(char* buffer,
                                 std::size_t len,
                                 const spot_sbe::MessageHeader& header,
                                 BsbeTrade* out,
                                 std::size_t out_cap,
                                 std::size_t* consumed) {
  if (out_cap == 0) {
    return kErrOutputTruncated;
  }

  spot_stream::TradesStreamEvent event;
  try {
    event.wrapForDecode(
        buffer, kHeaderSize, header.blockLength(), header.version(), len);
  } catch (const std::exception&) {
    return kErrCorrupt;
  }

  const std::int32_t price_exp = event.priceExponent();
  const std::int32_t qty_exp = event.qtyExponent();
  const std::int64_t transact_time = event.transactTime();
  const std::uint64_t fallback_ns = to_ns_signed(event.eventTime());

  auto& trades_group = event.trades();
  std::size_t index = 0;
  while (trades_group.hasNext()) {
    trades_group.next();
    if (index >= out_cap) {
      try {
        while (trades_group.hasNext()) {
          trades_group.next();
        }
      } catch (...) {
      }
      try {
        event.skipSymbol();
      } catch (const std::exception&) {
        return kErrCorrupt;
      }
      *consumed = event.sbePosition();
      return kErrOutputTruncated;
    }

    BsbeTrade& dst = out[index];
    dst.px_e8 = scale_to_e8(trades_group.price(), price_exp);
    dst.qty_e8 = scale_to_e8(trades_group.qty(), qty_exp);
    dst.side =
        trades_group.isBuyerMaker() == spot_stream::BoolEnum::True ? 1 : 0;
    dst._pad0 = 0;
    dst._pad1 = 0;
    dst.symbol_id = 0;
    std::uint64_t ts_ns = to_ns_signed(transact_time);
    if (ts_ns == 0) {
      ts_ns = fallback_ns;
    }
    dst.event_ts_ns = ts_ns;
    dst.trade_id = static_cast<std::uint64_t>(trades_group.id());
    ++index;
  }

  try {
    event.skipSymbol();
  } catch (const std::exception&) {
    return kErrCorrupt;
  }

  *consumed = event.sbePosition();
  return static_cast<int>(index);
}

int decode_stream_trades_message(char* buffer,
                                 std::size_t len,
                                 const spot_sbe::MessageHeader& header,
                                 BsbeTrade* out,
                                 std::size_t out_cap,
                                 std::size_t* consumed);

int decode_nested_message(const char* payload,
                          std::size_t payload_len,
                          BsbeTrade* out,
                          std::size_t out_cap) {
  if (payload_len < kHeaderSize) {
    return kErrCorrupt;
  }

  auto nested_buffer = const_cast<char*>(payload);
  spot_sbe::MessageHeader nested_header;
  try {
    nested_header.wrap(nested_buffer, 0, 0, payload_len);
  } catch (const std::exception&) {
    return kErrCorrupt;
  }

  const auto nested_schema_id = nested_header.schemaId();
  if (nested_schema_id == kSpotSchemaId &&
      nested_header.templateId() == kTradesTemplateId) {
    std::size_t nested_consumed = 0;
    int rc = decode_trades_payload(nested_buffer,
                                   payload_len,
                                   nested_header.blockLength(),
                                   nested_header.version(),
                                   out,
                                   out_cap,
                                   &nested_consumed);
    (void)nested_consumed;
    return rc;
  }

  if (nested_schema_id == kStreamSchemaId &&
      nested_header.templateId() == kStreamTradesTemplateId) {
    std::size_t nested_consumed = 0;
    int rc = decode_stream_trades_message(nested_buffer,
                                          payload_len,
                                          nested_header,
                                          out,
                                          out_cap,
                                          &nested_consumed);
    (void)nested_consumed;
    return rc;
  }

  if (nested_schema_id == kSpotSchemaId || nested_schema_id == kStreamSchemaId) {
    return 0;
  }

  return kErrSchemaMismatch;
}

int decode_ws_response(char* buffer,
                       std::size_t len,
                       const spot_sbe::MessageHeader& header,
                       BsbeTrade* out,
                       std::size_t out_cap,
                       std::size_t* consumed) {
  spot_sbe::WebSocketResponse message;
  try {
    message.wrapForDecode(
        buffer, kHeaderSize, header.blockLength(), header.version(), len);
  } catch (const std::exception&) {
    return kErrCorrupt;
  }

  try {
    auto& rate_limits = message.rateLimits();
    while (rate_limits.hasNext()) {
      rate_limits.next();
    }
  } catch (const std::exception&) {
    return kErrCorrupt;
  }

  try {
    message.skipId();
  } catch (const std::exception&) {
    return kErrCorrupt;
  }

  std::uint32_t result_len = 0;
  try {
    result_len = message.resultLength();
  } catch (const std::exception&) {
    return kErrCorrupt;
  }

  const std::uint64_t remaining =
      message.sbePosition() + 4ULL + static_cast<std::uint64_t>(result_len);
  if (remaining > len) {
    return kErrIncomplete;
  }

  const char* nested_ptr = nullptr;
  try {
    nested_ptr = message.result();
  } catch (const std::exception&) {
    return kErrCorrupt;
  }

  int rc = decode_nested_message(nested_ptr, result_len, out, out_cap);
  *consumed = message.sbePosition();
  return rc;
}

int decode_trades_message(char* buffer,
                          std::size_t len,
                          const spot_sbe::MessageHeader& header,
                          BsbeTrade* out,
                          std::size_t out_cap,
                          std::size_t* consumed) {
  std::size_t inner_consumed = 0;
  int rc = decode_trades_payload(buffer,
                                 len,
                                 header.blockLength(),
                                 header.version(),
                                 out,
                                 out_cap,
                                 &inner_consumed);
  *consumed = inner_consumed;
  return rc;
}

}  // namespace

// Expose C symbols so Rust can link without C++ name mangling.
extern "C" int bsbe_try_decode_one(const std::uint8_t* buf,
                                   std::size_t len,
                                   BsbeTrade* out,
                                   std::size_t out_cap,
                                   std::size_t* consumed) {
  if (consumed == nullptr || buf == nullptr) {
    return kErrCorrupt;
  }
  *consumed = 0;

  if (out == nullptr || out_cap == 0) {
    return kErrOutputTruncated;
  }

  if (len < kHeaderSize) {
    return kErrIncomplete;
  }

  auto buffer = reinterpret_cast<char*>(const_cast<std::uint8_t*>(buf));

  spot_sbe::MessageHeader header;
  try {
    header.wrap(buffer, 0, 0, len);
  } catch (const std::exception&) {
    return kErrCorrupt;
  }

  const auto schema_id = header.schemaId();
  if (schema_id == kSpotSchemaId) {
    if (header.templateId() == kWsTemplateId) {
      return decode_ws_response(buffer, len, header, out, out_cap, consumed);
    }

    if (header.templateId() == kTradesTemplateId) {
      return decode_trades_message(buffer, len, header, out, out_cap, consumed);
    }

    const std::size_t skip = std::min<std::size_t>(
        len, static_cast<std::size_t>(header.blockLength()) + kHeaderSize);
    *consumed = skip;
    return 0;
  }

  if (schema_id == kStreamSchemaId) {
    if (header.templateId() == kStreamTradesTemplateId) {
      return decode_stream_trades_message(
          buffer, len, header, out, out_cap, consumed);
    }

    const std::size_t skip = std::min<std::size_t>(
        len, static_cast<std::size_t>(header.blockLength()) + kHeaderSize);
    *consumed = skip;
    return 0;
  }

  return kErrSchemaMismatch;
}
