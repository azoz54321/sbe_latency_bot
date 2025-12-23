use rust_decimal::Decimal;

fn safe_multiplier(numerator: Decimal, denominator: Decimal, fallback: Decimal) -> Decimal {
    if denominator <= Decimal::ZERO {
        fallback
    } else {
        (numerator / denominator).max(Decimal::ZERO)
    }
}

pub fn tp_target_px(
    entry: Decimal,
    tp_pct: Decimal,
    maker_fee_pct: Decimal,
    taker_fee_pct: Decimal,
) -> Decimal {
    if entry <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let growth = (Decimal::ONE + tp_pct).max(Decimal::ZERO);
    let buy_fee = (Decimal::ONE + taker_fee_pct).max(Decimal::ZERO);
    let numerator = (growth * buy_fee).max(Decimal::ZERO);
    let fallback = (entry * growth).max(Decimal::ZERO);
    let multiplier = safe_multiplier(numerator, Decimal::ONE - maker_fee_pct, fallback / entry);
    (entry * multiplier).max(entry).normalize()
}

pub fn sl_trigger_px(
    entry: Decimal,
    sl_pct: Decimal,
    _maker_fee_pct: Decimal,
    taker_fee_pct: Decimal,
) -> Decimal {
    if entry <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let loss_component = (Decimal::ONE - sl_pct).max(Decimal::ZERO);
    let buy_fee = (Decimal::ONE + taker_fee_pct).max(Decimal::ZERO);
    let numerator = (loss_component * buy_fee).max(Decimal::ZERO);
    let fallback = (entry * loss_component).max(Decimal::ZERO);
    let multiplier = safe_multiplier(numerator, Decimal::ONE - taker_fee_pct, fallback / entry);
    (entry * multiplier)
        .max(Decimal::ZERO)
        .min(entry)
        .normalize()
}

pub fn breakeven_px(entry: Decimal, _maker_fee_pct: Decimal, taker_fee_pct: Decimal) -> Decimal {
    if entry <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let numerator = (Decimal::ONE + taker_fee_pct).max(Decimal::ZERO);
    let multiplier = safe_multiplier(numerator, Decimal::ONE - taker_fee_pct, Decimal::ONE);
    let result = (entry * multiplier).max(Decimal::ZERO);
    // bounce break-even should not exceed entry after fees
    result.min(entry).normalize()
}
