// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use crate::{TableError, Variant, VariantMutRef};

/// Trait for updating a variant in a table.
/// This trait defines a method to update a variant in a table,
/// given the original variant and a new variant.
/// The method returns a `Result` indicating success or failure.
///
/// # Returns
/// - `Result<(), TableError>`: Returns `Ok(())` if the update is successful,
pub trait Updater {
  /// update original variant with a new variant, change internal state if needed
  fn update(&mut self, original: VariantMutRef, new: Variant) -> Result<(), TableError>;

  /// update original variant with a new variant, change internal state if needed, with some context
  fn update_with_ctx(
    &mut self,
    original: VariantMutRef,
    new: Variant,
    _context: Option<UpdaterContext>,
  ) -> Result<(), TableError> {
    // Default implementation calls the basic update method.
    self.update(original, new)
  }

  /// reset internal state
  fn reset(&mut self) {
    // Default implementation does nothing.
    // This can be overridden by specific updaters if needed.
  }
}

/// represents a time period for updater do some operation periodically
#[derive(Debug, Clone, Default)]
pub struct TimePeriod {
  /// period in seconds
  pub period: u64,
  /// start time in seconds
  pub start: u64,
}

/// Context for the updater, can be used to pass additional information
#[derive(Debug, Clone, Default)]
pub struct UpdaterContext {
  /// current time in seconds
  pub now: u64,
}

impl UpdaterContext {
  /// create a new context with the current time, defaulting other fields
  pub fn now(now: u64) -> Self {
    Self {
      now,
      ..Default::default()
    }
  }
  /// set the current time in the context
  pub fn with_now(self, now: u64) -> Self {
    Self { now, ..self }
  }
}

mod stateful;
mod stateless;

pub use stateful::*;
pub use stateless::*;
