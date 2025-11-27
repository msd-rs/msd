use std::ops::Sub;

use crate::{TableError, Variant, VariantMutRef, updater::Updater};

/// Updater that keeps the first value after reset
pub struct KeepFirst(Option<Variant>);

impl Updater for KeepFirst {
  fn update(&mut self, original: VariantMutRef, new: Variant) -> Result<(), TableError> {
    match self.0.as_ref() {
      Some(v) => {
        // If we already have a value, we do not update it.
        original.set(v.clone())
      }
      None => {
        // If we do not have a value, we set it to the new value.
        self.0 = Some(new.clone());
        original.set(new)
      }
    }
  }

  fn reset(&mut self) {
    // Reset the internal state to None, so that the next update will set a new value.
    self.0 = None;
  }
}

/// Updater that calculate the difference between the first value and the new value
pub struct DiffFirst(Option<Variant>);

impl Updater for DiffFirst {
  fn update(&mut self, original: VariantMutRef, new: Variant) -> Result<(), TableError> {
    match self.0.as_ref() {
      Some(v) => {
        // If we already have a value, we calculate the difference.
        let diff = new.sub(v);
        original.set(diff)
      }
      None => {
        // If we do not have a value, we set it to the new value.
        self.0 = Some(new.clone());
        original.set(new)
      }
    }
  }

  fn reset(&mut self) {
    // Reset the internal state to None, so that the next update will set a new value.
    self.0 = None;
  }
}
