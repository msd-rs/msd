use std::ops::Add;

use crate::{TableError, Variant, VariantMutRef, updater::Updater};

/// An updater that assigns a new value to an existing variant if the index is same
pub struct AssignUpdater;

impl Updater for AssignUpdater {
  fn update(&mut self, original: VariantMutRef, new: Variant) -> Result<(), TableError> {
    original.set(new)
  }
}

/// An updater that accumulates a new value to an existing variant using the `Add` trait.
pub struct AccUpdater;

impl Updater for AccUpdater {
  fn update(&mut self, original: VariantMutRef, new: Variant) -> Result<(), TableError> {
    let lhs = original.to_variant();
    original.set(lhs.add(new))
  }
}

/// An updater that picks the maximum value between the original and the new variant.
pub struct MaxUpdater;

impl Updater for MaxUpdater {
  fn update(&mut self, original: VariantMutRef, new: Variant) -> Result<(), TableError> {
    let lhs = original.to_variant();
    match lhs.partial_cmp(&new) {
      Some(std::cmp::Ordering::Less) => original.set(new),
      _ => Ok(()),
    }
  }
}

/// An updater that picks the minimum value between the original and the new variant.
pub struct MinUpdater;

impl Updater for MinUpdater {
  fn update(&mut self, original: VariantMutRef, new: Variant) -> Result<(), TableError> {
    let lhs = original.to_variant();
    match lhs.partial_cmp(&new) {
      Some(std::cmp::Ordering::Greater) => original.set(new),
      _ => Ok(()),
    }
  }
}
