// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::ops::Sub;

use crate::{TableError, Variant, VariantMutRef, updater::Updater};

/// Updater that calculate the difference between the previous value and the new value
pub struct DiffPrevious(Option<Variant>);

impl Updater for DiffPrevious {
  fn update(&mut self, original: VariantMutRef, new: Variant) -> Result<(), TableError> {
    match self.0.as_ref() {
      Some(v) => {
        // If we already have a value, we calculate the difference.
        let diff = new.sub(v);
        original.set(diff)
      }
      None => {
        // If we do not have a value, we set it to the new value.
        let zero = new.zero_value();
        self.0 = Some(new);
        original.set(zero)
      }
    }
  }

  fn reset(&mut self) {
    // Reset the internal state to None, so that the next update will set a new value.
    self.0 = None;
  }
}
