// Copyright 2022 RisingWave Labs
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

use std::borrow::Cow;
use std::fmt::Display;
use std::hash::{BuildHasher, Hasher};
use std::ops::RangeBounds;

use bytes::{BufMut, Bytes, BytesMut};
use itertools::Itertools;

use self::empty::EMPTY;
use crate::hash::HashCode;
use crate::types::{DatumRef, ToDatumRef, ToOwnedDatum, ToText, hash_datum};
use crate::util::iter_util::ZipEqDebug;
use crate::util::row_serde::OrderedRowSerde;
use crate::util::value_encoding;

/// The trait for abstracting over a Row-like type.
pub trait Row: Sized + std::fmt::Debug + PartialEq + Eq + Send + Sync {
    /// Returns the [`DatumRef`] at the given `index`.
    fn datum_at(&self, index: usize) -> DatumRef<'_>;

    /// Returns the [`DatumRef`] at the given `index` without bounds checking.
    ///
    /// # Safety
    /// Calling this method with an out-of-bounds index is undefined behavior.
    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_>;

    /// Returns the number of datum in the row.
    fn len(&self) -> usize;

    /// Returns `true` if the row contains no datum.
    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns an iterator over the datums in the row, in [`DatumRef`] form.
    fn iter(&self) -> impl Iterator<Item = DatumRef<'_>>;

    /// Converts the row into an [`OwnedRow`].
    ///
    /// Prefer `into_owned_row` if the row is already owned.
    #[inline]
    fn to_owned_row(&self) -> OwnedRow {
        OwnedRow::new(self.iter().map(|d| d.to_owned_datum()).collect())
    }

    /// Consumes `self` and converts it into an [`OwnedRow`].
    #[inline]
    fn into_owned_row(self) -> OwnedRow {
        self.to_owned_row()
    }

    /// Serializes the row with value encoding, into the given `buf`.
    #[inline]
    fn value_serialize_into(&self, mut buf: impl BufMut) {
        for datum in self.iter() {
            value_encoding::serialize_datum_into(datum, &mut buf);
        }
    }

    /// Serializes the row with value encoding and returns the bytes.
    #[inline]
    fn value_serialize(&self) -> Vec<u8> {
        let estimate_size = self
            .iter()
            .map(value_encoding::estimate_serialize_datum_size)
            .sum();
        let mut buf = Vec::with_capacity(estimate_size);
        self.value_serialize_into(&mut buf);
        buf
    }

    /// Serializes the row with value encoding and returns the bytes.
    #[inline]
    fn value_serialize_bytes(&self) -> Bytes {
        let estimate_size = self
            .iter()
            .map(value_encoding::estimate_serialize_datum_size)
            .sum();
        let mut buf = BytesMut::with_capacity(estimate_size);
        self.value_serialize_into(&mut buf);
        buf.freeze()
    }

    fn value_estimate_size(&self) -> usize {
        self.iter()
            .map(value_encoding::estimate_serialize_datum_size)
            .sum()
    }

    /// Serializes the row with memcomparable encoding, into the given `buf`. As each datum may have
    /// different order type, a `serde` should be provided.
    #[inline]
    fn memcmp_serialize_into(&self, serde: &OrderedRowSerde, buf: impl BufMut) {
        serde.serialize(self, buf);
    }

    /// Serializes the row with memcomparable encoding and return the bytes. As each datum may have
    /// different order type, a `serde` should be provided.
    #[inline]
    fn memcmp_serialize(&self, serde: &OrderedRowSerde) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.len()); // each datum is at least 1 byte
        self.memcmp_serialize_into(serde, &mut buf);
        buf
    }

    /// Hash the datums of this row into the given hasher.
    ///
    /// Implementors should delegate [`std::hash::Hash::hash`] to this method.
    fn hash_datums_into<H: Hasher>(&self, state: &mut H) {
        for datum in self.iter() {
            hash_datum(datum, state);
        }
    }

    /// Returns the hash code of the row.
    fn hash<H: BuildHasher>(&self, hash_builder: H) -> HashCode<H> {
        let mut hasher = hash_builder.build_hasher();
        self.hash_datums_into(&mut hasher);
        hasher.finish().into()
    }

    /// Determines whether the datums of this row are equal to those of another.
    #[inline]
    fn eq(this: &Self, other: impl Row) -> bool {
        if this.len() != other.len() {
            return false;
        }
        for i in (0..this.len()).rev() {
            // compare from the end to the start, as it's more likely to have same prefix
            // SAFETY: index is in bounds as we are iterating from 0 to len.
            if unsafe { this.datum_at_unchecked(i) != other.datum_at_unchecked(i) } {
                return false;
            }
        }
        true
    }
}

const fn assert_row<R: Row>(r: R) -> R {
    r
}

/// An extension trait for [`Row`]s that provides a variety of convenient adapters.
pub trait RowExt: Row {
    /// Adapter for chaining two rows together.
    fn chain<R: Row>(self, other: R) -> Chain<Self, R>
    where
        Self: Sized,
    {
        assert_row(Chain::new(self, other))
    }

    /// Adapter for projecting a row onto a subset of its columns with the given `indices`.
    ///
    /// # Panics
    /// Panics if `indices` contains an out-of-bounds index.
    fn project(self, indices: &[usize]) -> Project<'_, Self>
    where
        Self: Sized,
    {
        assert_row(Project::new(self, indices))
    }

    /// Adapter for slicing a row with the given `range`.
    ///
    /// # Panics
    /// Panics if range is out of bounds.
    fn slice(self, range: impl RangeBounds<usize>) -> Slice<Self>
    where
        Self: Sized,
    {
        assert_row(Slice::new(self, range))
    }

    /// Returns a displayable wrapper for the row.
    fn display(&self) -> impl Display + '_ {
        struct D<'a, T: Row>(&'a T);
        impl<T: Row> Display for D<'_, T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "[{}]",
                    self.0.iter().format_with(", ", |datum, f| {
                        match datum {
                            None => f(&"NULL"),
                            Some(scalar) => f(&scalar.text_display()),
                        }
                    })
                )
            }
        }
        D(self)
    }

    fn is_null_at(&self, index: usize) -> bool {
        self.datum_at(index).is_none()
    }
}

impl<R: Row> RowExt for R {}

/// Forward the implementation of [`Row`] to the deref target.
macro_rules! deref_forward_row {
    () => {
        fn datum_at(&self, index: usize) -> crate::types::DatumRef<'_> {
            (**self).datum_at(index)
        }

        unsafe fn datum_at_unchecked(&self, index: usize) -> crate::types::DatumRef<'_> {
            unsafe { (**self).datum_at_unchecked(index) }
        }

        fn len(&self) -> usize {
            (**self).len()
        }

        fn is_empty(&self) -> bool {
            (**self).is_empty()
        }

        fn iter(&self) -> impl Iterator<Item = crate::types::DatumRef<'_>> {
            (**self).iter()
        }

        fn to_owned_row(&self) -> OwnedRow {
            (**self).to_owned_row()
        }

        fn value_serialize_into(&self, buf: impl bytes::BufMut) {
            (**self).value_serialize_into(buf)
        }

        fn value_serialize(&self) -> Vec<u8> {
            (**self).value_serialize()
        }

        fn memcmp_serialize_into(
            &self,
            serde: &$crate::util::row_serde::OrderedRowSerde,
            buf: impl bytes::BufMut,
        ) {
            (**self).memcmp_serialize_into(serde, buf)
        }

        fn memcmp_serialize(&self, serde: &$crate::util::row_serde::OrderedRowSerde) -> Vec<u8> {
            (**self).memcmp_serialize(serde)
        }

        fn hash<H: std::hash::BuildHasher>(&self, hash_builder: H) -> $crate::hash::HashCode<H> {
            (**self).hash(hash_builder)
        }

        fn hash_datums_into<H: std::hash::Hasher>(&self, state: &mut H) {
            (**self).hash_datums_into(state)
        }

        fn eq(this: &Self, other: impl Row) -> bool {
            Row::eq(&(**this), other)
        }
    };
}

impl<R: Row> Row for &R {
    deref_forward_row!();
}

impl<R: Row + Clone> Row for Cow<'_, R> {
    deref_forward_row!();

    // Manually implemented in case `R` has a more efficient implementation.
    fn into_owned_row(self) -> OwnedRow {
        self.into_owned().into_owned_row()
    }
}

impl<R: Row> Row for Box<R> {
    deref_forward_row!();

    // Manually implemented in case the `Cow` is `Owned` and `R` has a more efficient
    // implementation.
    fn into_owned_row(self) -> OwnedRow {
        (*self).into_owned_row()
    }
}

/// Implements [`Row`] for a slice of datums.
macro_rules! impl_slice_row {
    () => {
        #[inline]
        fn datum_at(&self, index: usize) -> DatumRef<'_> {
            self[index].to_datum_ref()
        }

        #[inline]
        unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
            unsafe { self.get_unchecked(index).to_datum_ref() }
        }

        #[inline]
        fn len(&self) -> usize {
            AsRef::<[D]>::as_ref(self).len()
        }

        #[inline]
        fn iter(&self) -> impl Iterator<Item = DatumRef<'_>> {
            AsRef::<[D]>::as_ref(self)
                .iter()
                .map(ToDatumRef::to_datum_ref)
        }
    };
}

impl<D: ToDatumRef> Row for &[D] {
    impl_slice_row!();
}

impl<D: ToDatumRef, const N: usize> Row for [D; N] {
    impl_slice_row!();
}

impl<D: ToDatumRef + Default, const N: usize> Row for ArrayVec<[D; N]> {
    impl_slice_row!();
}

/// Implements [`Row`] for an optional row.
impl<R: Row> Row for Option<R> {
    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        match self {
            Some(row) => row.datum_at(index),
            None => EMPTY.datum_at(index),
        }
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        unsafe {
            match self {
                Some(row) => row.datum_at_unchecked(index),
                None => EMPTY.datum_at_unchecked(index),
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Some(row) => row.len(),
            None => 0,
        }
    }

    fn iter(&self) -> impl Iterator<Item = DatumRef<'_>> {
        match self {
            Some(row) => either::Either::Left(row.iter()),
            None => either::Either::Right(EMPTY.iter()),
        }
    }

    fn to_owned_row(&self) -> OwnedRow {
        match self {
            Some(row) => row.to_owned_row(),
            None => OwnedRow::new(Vec::new()),
        }
    }

    fn into_owned_row(self) -> OwnedRow {
        match self {
            Some(row) => row.into_owned_row(),
            None => OwnedRow::new(Vec::new()),
        }
    }

    fn value_serialize_into(&self, buf: impl BufMut) {
        if let Some(row) = self {
            row.value_serialize_into(buf);
        }
    }

    fn memcmp_serialize_into(&self, serde: &OrderedRowSerde, buf: impl BufMut) {
        if let Some(row) = self {
            row.memcmp_serialize_into(serde, buf);
        }
    }
}

/// Implements [`Row`] for an [`either::Either`] of two different types of rows.
impl<R1: Row, R2: Row> Row for either::Either<R1, R2> {
    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        either::for_both!(self, row => row.datum_at(index))
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        unsafe { either::for_both!(self, row => row.datum_at_unchecked(index)) }
    }

    fn len(&self) -> usize {
        either::for_both!(self, row => row.len())
    }

    fn is_empty(&self) -> bool {
        either::for_both!(self, row => row.is_empty())
    }

    fn iter(&self) -> impl Iterator<Item = DatumRef<'_>> {
        self.as_ref().map_either(Row::iter, Row::iter)
    }

    fn to_owned_row(&self) -> OwnedRow {
        either::for_both!(self, row => row.to_owned_row())
    }

    fn into_owned_row(self) -> OwnedRow {
        either::for_both!(self, row => row.into_owned_row())
    }

    fn value_serialize_into(&self, buf: impl BufMut) {
        either::for_both!(self, row => row.value_serialize_into(buf))
    }

    fn value_serialize(&self) -> Vec<u8> {
        either::for_both!(self, row => row.value_serialize())
    }

    fn value_serialize_bytes(&self) -> Bytes {
        either::for_both!(self, row => row.value_serialize_bytes())
    }

    fn memcmp_serialize_into(&self, serde: &OrderedRowSerde, buf: impl BufMut) {
        either::for_both!(self, row => row.memcmp_serialize_into(serde, buf))
    }

    fn memcmp_serialize(&self, serde: &OrderedRowSerde) -> Vec<u8> {
        either::for_both!(self, row => row.memcmp_serialize(serde))
    }

    fn hash_datums_into<H: Hasher>(&self, state: &mut H) {
        either::for_both!(self, row => row.hash_datums_into(state))
    }

    fn hash<H: BuildHasher>(&self, hash_builder: H) -> HashCode<H> {
        either::for_both!(self, row => row.hash(hash_builder))
    }

    fn eq(this: &Self, other: impl Row) -> bool {
        either::for_both!(this, row => Row::eq(row, other))
    }
}

mod chain;
mod compacted_row;
mod empty;
mod once;
mod ordered;
mod owned_row;
mod project;
mod repeat_n;
mod slice;
pub use ::tinyvec::ArrayVec;
pub use chain::Chain;
pub use compacted_row::CompactedRow;
pub use empty::{Empty, empty};
pub use once::{Once, once};
pub use owned_row::{OwnedRow, RowDeserializer};
pub use project::Project;
pub use repeat_n::{RepeatN, repeat_n};
pub use slice::Slice;

/// Whether two rows hold the same values with the same representation, so that no expression can
/// tell them apart. Stronger than [`Row::eq`], which follows the SQL equality: `1.0` and `1.00`
/// (`numeric`), `0` and `-0` (floats), `'1 day'` and `'24 hours'` (`interval`) are equal but not
/// identical. An update from one to the other is not a no-op.
pub fn identical(this: impl Row, other: impl Row) -> bool {
    this.len() == other.len()
        && this
            .iter()
            .zip_eq_debug(other.iter())
            .all(|(a, b)| datum_identical(a, b))
}

/// Whether two datums hold the same value with the same representation, see [`identical`].
pub fn datum_identical(a: DatumRef<'_>, b: DatumRef<'_>) -> bool {
    use crate::types::ScalarRefImpl;
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => {
            a == b
                && match a {
                    // Equal values of these types have a single representation.
                    ScalarRefImpl::Int16(_)
                    | ScalarRefImpl::Int32(_)
                    | ScalarRefImpl::Int64(_)
                    | ScalarRefImpl::Int256(_)
                    | ScalarRefImpl::Serial(_)
                    | ScalarRefImpl::Bool(_)
                    | ScalarRefImpl::Utf8(_)
                    | ScalarRefImpl::Bytea(_)
                    | ScalarRefImpl::Date(_)
                    | ScalarRefImpl::Time(_)
                    | ScalarRefImpl::Timestamp(_)
                    | ScalarRefImpl::Timestamptz(_) => true,
                    // The others may differ in their text while equal.
                    _ => a.to_text() == b.to_text(),
                }
        }
        _ => false,
    }
}

#[cfg(test)]
mod identical_tests {
    use super::*;
    use crate::types::{Decimal, Interval, ScalarImpl};

    #[test]
    fn test_identical() {
        let row = |datum: ScalarImpl| OwnedRow::new(vec![Some(ScalarImpl::Int32(1)), Some(datum)]);
        let one_0: Decimal = "1.0".parse().unwrap();
        let one_00: Decimal = "1.00".parse().unwrap();
        assert!(Row::eq(&row(one_0.into()), &row(one_00.into())));
        assert!(!identical(&row(one_0.into()), &row(one_00.into())));
        assert!(identical(&row(one_0.into()), &row(one_0.into())));

        let zero = ScalarImpl::from(0.0f64);
        let neg_zero = ScalarImpl::from(-0.0f64);
        assert!(!identical(&row(zero.clone()), &row(neg_zero)));
        assert!(identical(&row(zero.clone()), &row(zero)));

        let day = ScalarImpl::Interval(Interval::from_month_day_usec(0, 1, 0));
        let hours =
            ScalarImpl::Interval(Interval::from_month_day_usec(0, 0, 24 * 3600 * 1_000_000));
        assert!(!identical(&row(day.clone()), &row(hours)));
        assert!(identical(&row(day.clone()), &row(day)));

        assert!(!identical(
            &OwnedRow::new(vec![None]),
            &OwnedRow::new(vec![Some(ScalarImpl::Int32(1))])
        ));
    }
}
