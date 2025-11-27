use std::ops::{Add, Div};

use crate::{
  TableError, Variant, VariantMutRef,
  updater::{TimePeriod, Updater, UpdaterContext},
};

/// An updater that accumulates a new value to an existing variant in a specified time period.
///
/// - use `with_period` to set the time period for accumulation.
/// - use `update_with_ctx` to provide a context with a "now" key to check if the period has expired.
#[derive(Debug, Clone, Default)]
pub struct PeriodAccUpdater {
  /// Internal state to hold the accumulated value.
  accumulated: Option<Variant>,
  period: Option<TimePeriod>,
}

impl PeriodAccUpdater {
  /// Add a time period to the updater.
  pub fn with_period(self, period: TimePeriod) -> Self {
    Self {
      period: Some(period),
      ..self
    }
  }
}

impl Updater for PeriodAccUpdater {
  fn update(&mut self, original: VariantMutRef, new: Variant) -> Result<(), TableError> {
    match self.accumulated.as_ref() {
      Some(v) => {
        // If we already have a value, we accumulate the new value.
        let accumulated = new.add(v);
        original.set(accumulated)
      }
      None => {
        // If we do not have a value, we set it to the new value.
        self.accumulated = Some(new.clone());
        original.set(new)
      }
    }
  }

  fn update_with_ctx(
    &mut self,
    original: VariantMutRef,
    new: Variant,
    context: Option<UpdaterContext>,
  ) -> Result<(), TableError> {
    // if there is period, and context has "now" key, check if the period has expired
    if let Some(period) = self.period.as_mut()
      && let Some(now) = context.as_ref().map(|ctx| ctx.now)
      && period.start + period.period < now
    {
      period.start = now;
      self.reset();
    }
    // Update the accumulated value.
    self.update(original, new)
  }

  fn reset(&mut self) {
    // Reset the internal state to None, so that the next update will set a new value.
    self.accumulated = None;
    if let Some(period) = &mut self.period {
      // Reset the period to its initial state.
      period.start = 0;
    }
  }
}

/// An updater that calculates the average value in a specified range.
pub struct PeriodAvgUpdater {
  /// Internal state to hold the accumulated sum and count of values.
  accumulated: Option<Variant>,
  /// The number of times the value has been updated.
  times: usize,
  period: Option<TimePeriod>,
}

impl PeriodAvgUpdater {
  /// Add a time period to the updater.
  pub fn with_period(self, period: TimePeriod) -> Self {
    Self {
      period: Some(period),
      ..self
    }
  }
}

impl Updater for PeriodAvgUpdater {
  fn update(&mut self, original: VariantMutRef, new: Variant) -> Result<(), TableError> {
    match self.accumulated.as_mut() {
      Some(sum) => {
        // If we already have a value, we accumulate the new value.
        let new_sum = new.add(sum.clone());
        self.times += 1;
        original.set(new_sum.div(v!(self.times)))
      }
      None => {
        // If we do not have a value, we set it to the new value.
        self.accumulated = Some(new.clone());
        self.times = 1;
        original.set(new)
      }
    }
  }

  fn update_with_ctx(
    &mut self,
    original: VariantMutRef,
    new: Variant,
    context: Option<UpdaterContext>,
  ) -> Result<(), TableError> {
    // if there is period, and context has "now" key, check if the period has expired
    if let Some(period) = self.period.as_mut()
      && let Some(now) = context.as_ref().map(|ctx| ctx.now)
      && period.start + period.period < now
    {
      period.start = now;
      self.reset();
    }
    // Update the accumulated value.
    self.update(original, new)
  }

  fn reset(&mut self) {
    // Reset the internal state to None, so that the next update will set a new value.
    self.accumulated = None;
    self.times = 0;
  }
}

/// An updater that count the number of updates in a specified time period.
#[derive(Debug, Clone, Default)]
pub struct PeriodCountUpdater {
  /// The number of times the value has been updated.
  times: usize,
  period: Option<TimePeriod>,
}

impl PeriodCountUpdater {
  /// Add a time period to the updater.
  pub fn with_period(self, period: TimePeriod) -> Self {
    Self {
      period: Some(period),
      ..self
    }
  }
}

impl Updater for PeriodCountUpdater {
  fn update(&mut self, original: VariantMutRef, _new: Variant) -> Result<(), TableError> {
    // Increment the count of updates.
    self.times += 1;
    original.set(v!(self.times))
  }

  fn update_with_ctx(
    &mut self,
    original: VariantMutRef,
    new: Variant,
    context: Option<UpdaterContext>,
  ) -> Result<(), TableError> {
    // if there is period, and context has "now" key, check if the period has expired
    if let Some(period) = self.period.as_mut()
      && let Some(now) = context.as_ref().map(|ctx| ctx.now)
      && period.start + period.period < now
    {
      period.start = now;
      self.reset();
    }
    // Update the count.
    self.update(original, new)
  }

  fn reset(&mut self) {
    // Reset the count to zero.
    self.times = 0;
  }
}

/// An updater that calculates the maximum value in a specified range.
#[derive(Debug, Clone, Default)]
pub struct RangeMaxUpdater {
  /// Internal state to hold the accumulated value.
  max_value: Option<Variant>,
  period: Option<TimePeriod>,
}

impl RangeMaxUpdater {
  /// Add a time period to the updater.
  pub fn with_period(self, period: TimePeriod) -> Self {
    Self {
      period: Some(period),
      ..self
    }
  }
}

impl Updater for RangeMaxUpdater {
  fn update(&mut self, original: VariantMutRef, new: Variant) -> Result<(), TableError> {
    match self.max_value.as_ref() {
      Some(v) => {
        if v < &new {
          original.set(new)
        } else {
          Ok(())
        }
      }
      None => {
        // If we do not have a value, we set it to the new value.
        self.max_value = Some(new.clone());
        original.set(new)
      }
    }
  }

  fn update_with_ctx(
    &mut self,
    original: VariantMutRef,
    new: Variant,
    context: Option<UpdaterContext>,
  ) -> Result<(), TableError> {
    // if there is period, and context has "now" key, check if the period has expired
    if let Some(period) = self.period.as_mut()
      && let Some(now) = context.as_ref().map(|ctx| ctx.now)
      && period.start + period.period < now
    {
      period.start = now;
      self.reset();
    }
    // Update the max value.
    self.update(original, new)
  }

  fn reset(&mut self) {
    // Reset the internal state to None, so that the next update will set a new value.
    self.max_value = None;
  }
}

/// An updater that calculates the minimum value in a specified range.
#[derive(Debug, Clone, Default)]
pub struct RangeMinUpdater {
  /// Internal state to hold the accumulated value.
  min_value: Option<Variant>,
  period: Option<TimePeriod>,
}

impl RangeMinUpdater {
  /// Add a time period to the updater.
  pub fn with_period(self, period: TimePeriod) -> Self {
    Self {
      period: Some(period),
      ..self
    }
  }
}

impl Updater for RangeMinUpdater {
  fn update(&mut self, original: VariantMutRef, new: Variant) -> Result<(), TableError> {
    match self.min_value.as_ref() {
      Some(v) => {
        if v > &new {
          original.set(new)
        } else {
          Ok(())
        }
      }
      None => {
        // If we do not have a value, we set it to the new value.
        self.min_value = Some(new.clone());
        original.set(new)
      }
    }
  }

  fn update_with_ctx(
    &mut self,
    original: VariantMutRef,
    new: Variant,
    context: Option<UpdaterContext>,
  ) -> Result<(), TableError> {
    // if there is period, and context has "now" key, check if the period has expired
    if let Some(period) = self.period.as_mut()
      && let Some(now) = context.as_ref().map(|ctx| ctx.now)
      && period.start + period.period < now
    {
      period.start = now;
      self.reset();
    }
    // Update the min value.
    self.update(original, new)
  }

  fn reset(&mut self) {
    // Reset the internal state to None, so that the next update will set a new value.
    self.min_value = None;
  }
}
