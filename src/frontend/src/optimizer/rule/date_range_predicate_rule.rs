// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Range form of the comparisons of `date_trunc`, `extract(year ...)`, `date_part('year', ...)`
//! and casts to `date` with a constant (`batch_date_range_predicates`), Calcite's
//! `DateRangeRules`, for batch queries: the column is compared with constants, which the scan
//! turns into key ranges when the column is a prefix of the key of the table.

use chrono::{Datelike, Months, TimeDelta};
use risingwave_common::types::{DataType, Date, ScalarImpl, Timestamp, Timestamptz};

use super::prelude::{PlanRef, *};
use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall, Literal};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalFilter, PlanTreeNodeUnary};
use crate::utils::Condition;

/// Rewrites, in the predicate of a filter or of a scan, each top-level conjunct that compares
/// (`=`, `<`, `<=`, `>`, `>=`) one of the following functions of an expression `x` with a constant
/// into comparisons of `x` itself (`x >= lo`, `x < hi`):
///
/// - `date_trunc(unit, x)` for a `timestamp` `x`, or a `timestamptz` `x` truncated in the `UTC`
///   time zone, for the units `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter` and
///   `year`;
/// - `x::date` for a `timestamp` `x`, or a `timestamptz` `x` in the `UTC` time zone;
/// - `extract(year from x)` and `date_part('year', x)` for a `date` or `timestamp` `x`, or a
///   `timestamptz` `x` in the `UTC` time zone.
///
/// A function of a `timestamptz` uses the session time zone, which is only written into the
/// expression (as a third argument, or `AT TIME ZONE`) after the logical optimization: both forms
/// are handled, the first one when the session time zone is `UTC`.
///
/// ```text
/// date_trunc('month', ts) = '2024-05-01'   =>  ts >= '2024-05-01' AND ts < '2024-06-01'
/// ts::date <= '2024-05-01'                 =>  ts < '2024-05-02 00:00:00'
/// extract(year from d) > 2023              =>  d >= '2024-01-01'
/// ```
///
/// Exact, for a truncation `f` that is non-decreasing, idempotent (`f(f(x)) = f(x)`) and below its
/// argument (`f(x) <= x`): for an aligned value `a` (`f(a) = a`), `f(x) < a` exactly when
/// `x < a`. With `up(c)` the smallest aligned value at or above `c` and `after(c)` the smallest
/// one above `c`: `f(x) < c` is `x < up(c)`, `f(x) >= c` is `x >= up(c)`, `f(x) <= c` is
/// `x < after(c)`, `f(x) > c` is `x >= after(c)`, and `f(x) = c` is `c <= x < after(c)` when `c` is
/// aligned (never true otherwise: then the conjunct is left as it is). The truncation is
/// computed with the functions of `date_trunc` itself, and the next aligned value by adding one
/// unit, checked to be aligned. A cast to `date` is the truncation to the day; the year of
/// `extract` is an integer `y(x)`, non-decreasing, with `y(x) < n` exactly when `x` is before
/// January 1 of the year `n` (for a year `n` from 1 to 9999, where both conventions for the years
/// before Christ agree that they are smaller). If `x` is `NULL`, both forms are `NULL`.
///
/// The bounds then go through the conversions of `x` that are increasing (`d::timestamp` of a date
/// `d`: `d::timestamp >= v` is `d >= ` the first day at or after `v`; `AT TIME ZONE 'UTC'` and the
/// casts between `timestamp`, `timestamptz` and `date` in a `UTC` session, which keep the value),
/// so that `date_trunc('month', d)` of a date column, bound as `date_trunc('month',
/// d::timestamptz)`, becomes a range of `d`.
///
/// Not rewritten: other time zones than `UTC` (a day or an hour may then be shorter or longer and
/// the truncation of a local time that does not exist or exists twice depends on the zone), other
/// units, an `x` that is not deterministic (it is written twice for `=`), comparisons inside other
/// expressions, and constants out of the years 1 to 9999.
pub struct DateRangePredicateRule;

impl Rule<Logical> for DateRangePredicateRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        // The session time zone is inlined into the expressions only after the logical
        // optimization: a `timestamptz` function without zone argument uses it.
        let utc_session = is_utc_name(&plan.ctx().get_session_timezone());
        if let Some(filter) = plan.as_logical_filter() {
            let predicate = rewrite_condition(filter.predicate(), utc_session)?;
            return Some(LogicalFilter::create(filter.input(), predicate));
        }
        let scan = plan.as_logical_scan()?;
        let predicate = rewrite_condition(scan.predicate(), utc_session)?;
        Some(scan.clone_with_predicate(predicate).into())
    }
}

impl DateRangePredicateRule {
    pub fn create() -> BoxedRule {
        Box::new(DateRangePredicateRule)
    }
}

fn rewrite_condition(condition: &Condition, utc_session: bool) -> Option<Condition> {
    let mut changed = false;
    let mut conjunctions = vec![];
    for expr in &condition.conjunctions {
        match range_form(expr, utc_session) {
            Some(range) => {
                conjunctions.extend(range);
                changed = true;
            }
            None => conjunctions.push(expr.clone()),
        }
    }
    changed.then_some(Condition { conjunctions })
}

/// A comparison, with the function call on the left.
#[derive(Clone, Copy, PartialEq)]
enum Cmp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Cmp {
    fn of(func_type: ExprType) -> Option<Self> {
        Some(match func_type {
            ExprType::Equal => Cmp::Eq,
            ExprType::LessThan => Cmp::Lt,
            ExprType::LessThanOrEqual => Cmp::Le,
            ExprType::GreaterThan => Cmp::Gt,
            ExprType::GreaterThanOrEqual => Cmp::Ge,
            _ => return None,
        })
    }

    /// The comparison with its operands swapped: `c < f(x)` is `f(x) > c`.
    fn flip(self) -> Self {
        match self {
            Cmp::Eq => Cmp::Eq,
            Cmp::Lt => Cmp::Gt,
            Cmp::Le => Cmp::Ge,
            Cmp::Gt => Cmp::Lt,
            Cmp::Ge => Cmp::Le,
        }
    }
}

/// The units of `date_trunc` that are handled.
#[derive(Clone, Copy)]
enum Unit {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl Unit {
    fn of(name: &str) -> Option<Self> {
        Some(match name.to_ascii_lowercase().as_str() {
            "second" => Unit::Second,
            "minute" => Unit::Minute,
            "hour" => Unit::Hour,
            "day" => Unit::Day,
            "week" => Unit::Week,
            "month" => Unit::Month,
            "quarter" => Unit::Quarter,
            "year" => Unit::Year,
            _ => return None,
        })
    }

    /// The truncation of `date_trunc(unit, timestamp)`.
    fn truncate(self, ts: Timestamp) -> Timestamp {
        match self {
            Unit::Second => ts.truncate_second(),
            Unit::Minute => ts.truncate_minute(),
            Unit::Hour => ts.truncate_hour(),
            Unit::Day => ts.truncate_day(),
            Unit::Week => ts.truncate_week(),
            Unit::Month => ts.truncate_month(),
            Unit::Quarter => ts.truncate_quarter(),
            Unit::Year => ts.truncate_year(),
        }
    }

    /// The next aligned value after the aligned value `ts`: `ts` plus one unit, checked.
    fn next(self, ts: Timestamp) -> Option<Timestamp> {
        let seconds = |n: i64| TimeDelta::try_seconds(n).and_then(|d| ts.0.checked_add_signed(d));
        let next = match self {
            Unit::Second => seconds(1),
            Unit::Minute => seconds(60),
            Unit::Hour => seconds(3_600),
            Unit::Day => seconds(86_400),
            Unit::Week => seconds(7 * 86_400),
            Unit::Month => ts.0.checked_add_months(Months::new(1)),
            Unit::Quarter => ts.0.checked_add_months(Months::new(3)),
            Unit::Year => ts.0.checked_add_months(Months::new(12)),
        }
        .map(Timestamp)?;
        (next > ts && self.truncate(next) == next).then_some(next)
    }

    fn is_aligned(self, ts: Timestamp) -> bool {
        self.truncate(ts) == ts
    }

    /// The smallest aligned value at or above `ts`.
    fn up(self, ts: Timestamp) -> Option<Timestamp> {
        if self.is_aligned(ts) {
            Some(ts)
        } else {
            self.next(self.truncate(ts))
        }
    }

    /// The smallest aligned value above `ts`.
    fn after(self, ts: Timestamp) -> Option<Timestamp> {
        self.next(self.truncate(ts))
    }
}

/// The bounds of `x` equivalent to `f(x) <cmp> c`, as (lower bound included, upper bound
/// excluded), for a truncation `f` to `unit`. Only for constants of the years 1 to 9999, far from
/// the limits of the dates.
fn truncation_bounds(
    unit: Unit,
    cmp: Cmp,
    c: Timestamp,
) -> Option<(Option<Timestamp>, Option<Timestamp>)> {
    if !(1..=9999).contains(&c.0.year()) {
        return None;
    }
    Some(match cmp {
        Cmp::Eq => {
            if !unit.is_aligned(c) {
                return None;
            }
            (Some(c), Some(unit.next(c)?))
        }
        Cmp::Lt => (None, Some(unit.up(c)?)),
        Cmp::Ge => (Some(unit.up(c)?), None),
        Cmp::Le => (None, Some(unit.after(c)?)),
        Cmp::Gt => (Some(unit.after(c)?), None),
    })
}

/// The first instant of January 1 of the year, for the years 1 to 9999.
fn year_start(year: i64) -> Option<Timestamp> {
    if !(1..=9999).contains(&year) {
        return None;
    }
    Some(Date::from_ymd_uncheck(year as i32, 1, 1).into())
}

/// The bounds of `x` equivalent to `y(x) <cmp> n`, for the year `y(x)` of `x` and a number `n`
/// given by its floor and ceiling.
fn year_bounds(cmp: Cmp, floor: i64, ceil: i64) -> Option<(Option<Timestamp>, Option<Timestamp>)> {
    Some(match cmp {
        Cmp::Eq => {
            if floor != ceil {
                return None;
            }
            (Some(year_start(floor)?), Some(year_start(floor + 1)?))
        }
        Cmp::Lt => (None, Some(year_start(ceil)?)),
        Cmp::Ge => (Some(year_start(ceil)?), None),
        Cmp::Le => (None, Some(year_start(floor + 1)?)),
        Cmp::Gt => (Some(year_start(floor + 1)?), None),
    })
}

/// The floor and the ceiling of a finite number, as integers, for numbers of at most a billion
/// (far beyond the years that are handled).
fn floor_ceil(value: &ScalarImpl) -> Option<(i64, i64)> {
    let (floor, ceil) = match value {
        ScalarImpl::Decimal(d) => (
            i64::try_from(d.floor()).ok()?,
            i64::try_from(d.ceil()).ok()?,
        ),
        ScalarImpl::Float64(f) => {
            let f = f.0;
            if !f.is_finite() || f.abs() > 1e9 {
                return None;
            }
            (f.floor() as i64, f.ceil() as i64)
        }
        _ => return None,
    };
    (floor.abs() <= 1_000_000_000 && ceil.abs() <= 1_000_000_000).then_some((floor, ceil))
}

/// Whether the time zone is `UTC`.
fn is_utc_name(zone: &str) -> bool {
    zone.eq_ignore_ascii_case("UTC") || zone.eq_ignore_ascii_case("Etc/UTC")
}

/// Whether the expression is the literal name of the `UTC` time zone.
fn is_utc(expr: &ExprImpl) -> bool {
    if let ExprImpl::Literal(literal) = expr
        && let Some(ScalarImpl::Utf8(zone)) = literal.get_data()
    {
        is_utc_name(zone)
    } else {
        false
    }
}

/// The text of a literal.
fn literal_text(expr: &ExprImpl) -> Option<&str> {
    if let ExprImpl::Literal(literal) = expr
        && let Some(ScalarImpl::Utf8(text)) = literal.get_data()
    {
        Some(&**text)
    } else {
        None
    }
}

/// The value of a constant expression, if not `NULL`.
fn constant(expr: &ExprImpl) -> Option<ScalarImpl> {
    if !expr.is_const() {
        return None;
    }
    expr.try_fold_const()?.ok()?
}

/// The value of a `timestamp`, or of a `timestamptz` read in `UTC`, as a `timestamp`.
fn as_timestamp(value: &ScalarImpl) -> Option<Timestamp> {
    match value {
        ScalarImpl::Timestamp(ts) => Some(*ts),
        ScalarImpl::Timestamptz(tz) => Some(Timestamp(tz.to_datetime_utc().naive_utc())),
        ScalarImpl::Date(date) => Some((*date).into()),
        _ => None,
    }
}

/// The conjuncts equivalent to a comparison of one of the handled functions with a constant.
/// `utc_session`: the session time zone, used by the `timestamptz` functions without zone
/// argument, is `UTC`.
fn range_form(expr: &ExprImpl, utc_session: bool) -> Option<Vec<ExprImpl>> {
    let call = expr.as_function_call()?;
    let cmp = Cmp::of(call.func_type())?;
    let [a, b] = call.inputs() else {
        return None;
    };
    let (operand, value, cmp) = match (constant(b), constant(a)) {
        (Some(value), _) => (a, value, cmp),
        (None, Some(value)) => (b, value, cmp.flip()),
        (None, None) => return None,
    };
    let operand = operand.as_function_call()?;
    let (x, (lower, upper)) = match (operand.func_type(), operand.inputs()) {
        (ExprType::DateTrunc, [unit, x])
            if x.return_type() == DataType::Timestamp
                && matches!(value, ScalarImpl::Timestamp(_)) =>
        {
            let unit = Unit::of(literal_text(unit)?)?;
            (x, truncation_bounds(unit, cmp, as_timestamp(&value)?)?)
        }
        (ExprType::DateTrunc, [unit, x])
            if utc_session
                && x.return_type() == DataType::Timestamptz
                && matches!(value, ScalarImpl::Timestamptz(_)) =>
        {
            let unit = Unit::of(literal_text(unit)?)?;
            (x, truncation_bounds(unit, cmp, as_timestamp(&value)?)?)
        }
        (ExprType::DateTrunc, [unit, x, zone])
            if x.return_type() == DataType::Timestamptz
                && is_utc(zone)
                && matches!(value, ScalarImpl::Timestamptz(_)) =>
        {
            let unit = Unit::of(literal_text(unit)?)?;
            (x, truncation_bounds(unit, cmp, as_timestamp(&value)?)?)
        }
        (ExprType::Cast, [x])
            if operand.return_type() == DataType::Date
                && (x.return_type() == DataType::Timestamp
                    || (utc_session && x.return_type() == DataType::Timestamptz))
                && matches!(value, ScalarImpl::Date(_)) =>
        {
            (x, truncation_bounds(Unit::Day, cmp, as_timestamp(&value)?)?)
        }
        (ExprType::Extract | ExprType::DatePart, [field, x])
            if matches!(x.return_type(), DataType::Date | DataType::Timestamp)
                || (utc_session && x.return_type() == DataType::Timestamptz) =>
        {
            if !literal_text(field)?.eq_ignore_ascii_case("year") {
                return None;
            }
            let (floor, ceil) = floor_ceil(&value)?;
            (x, year_bounds(cmp, floor, ceil)?)
        }
        (ExprType::Extract | ExprType::DatePart, [field, x, zone])
            if x.return_type() == DataType::Timestamptz && is_utc(zone) =>
        {
            if !literal_text(field)?.eq_ignore_ascii_case("year") {
                return None;
            }
            let (floor, ceil) = floor_ceil(&value)?;
            (x, year_bounds(cmp, floor, ceil)?)
        }
        _ => return None,
    };
    // `x` is written twice for `=`, and once more in any case: it must be deterministic.
    if !x.is_pure() {
        return None;
    }
    let mut conjunctions = vec![];
    if let Some(lower) = lower {
        conjunctions.push(bound(x, true, lower, utc_session)?);
    }
    if let Some(upper) = upper {
        conjunctions.push(bound(x, false, upper, utc_session)?);
    }
    Some(conjunctions)
}

/// `x >= value` (`lower`) or `x < value`, for a `date`, `timestamp` or `timestamptz` `x` whose
/// value is given as a `timestamp` (in `UTC` for a `timestamptz`), compared with `x` itself or
/// with the operand of an increasing conversion of `x` (see [`peel`]).
fn bound(x: &ExprImpl, lower: bool, value: Timestamp, utc_session: bool) -> Option<ExprImpl> {
    let (mut x, mut value) = (x.clone(), value);
    while let Some((inner, inner_value)) = peel(&x, value, utc_session) {
        x = inner;
        value = inner_value;
    }
    let data_type = x.return_type();
    let literal: ExprImpl = match data_type {
        DataType::Timestamp => Literal::new(Some(ScalarImpl::Timestamp(value)), data_type).into(),
        DataType::Timestamptz => {
            let micros = value.0.and_utc().timestamp_micros();
            let tz = Timestamptz::from_micros(micros)?;
            Literal::new(Some(ScalarImpl::Timestamptz(tz)), data_type).into()
        }
        DataType::Date if value.truncate_day() == value => {
            Literal::new(Some(ScalarImpl::Date(Date(value.0.date()))), data_type).into()
        }
        _ => return None,
    };
    let func_type = if lower {
        ExprType::GreaterThanOrEqual
    } else {
        ExprType::LessThan
    };
    Some(FunctionCall::new_unchecked(func_type, vec![x, literal], DataType::Boolean).into())
}

/// For an increasing conversion `x = g(y)`, the operand `y` and the bound of `y` equivalent to the
/// bound `value` of `x` (the same for `x >= value` and `x < value`): the smallest `v` such that
/// `g(v) >= value`.
///
/// - `y AT TIME ZONE 'UTC'`, from `timestamp` to `timestamptz` or back: the same value, since a
///   `timestamptz` value is given as a `timestamp` in `UTC`; so is a cast between `timestamp`
///   and `timestamptz` when the session time zone is `UTC`;
/// - `y::timestamp` of a `date` `y`, or `y::timestamptz` in a `UTC` session: the first day at or
///   after `value`.
fn peel(x: &ExprImpl, value: Timestamp, utc_session: bool) -> Option<(ExprImpl, Timestamp)> {
    let call = x.as_function_call()?;
    match (call.func_type(), call.inputs()) {
        (ExprType::Cast, [y])
            if utc_session
                && matches!(
                    (y.return_type(), call.return_type()),
                    (DataType::Timestamp, DataType::Timestamptz)
                        | (DataType::Timestamptz, DataType::Timestamp)
                ) =>
        {
            Some((y.clone(), value))
        }
        (ExprType::AtTimeZone, [y, zone])
            if is_utc(zone)
                && matches!(
                    (y.return_type(), call.return_type()),
                    (DataType::Timestamp, DataType::Timestamptz)
                        | (DataType::Timestamptz, DataType::Timestamp)
                ) =>
        {
            Some((y.clone(), value))
        }
        (ExprType::Cast, [y])
            if y.return_type() == DataType::Date
                && (call.return_type() == DataType::Timestamp
                    || (utc_session && call.return_type() == DataType::Timestamptz)) =>
        {
            let day = value.truncate_day();
            let first_day = if day == value {
                day
            } else {
                Unit::Day.next(day)?
            };
            Some((y.clone(), first_day))
        }
        _ => None,
    }
}
