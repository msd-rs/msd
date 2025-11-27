/// Macros for creating `Variant` instances.
/// This macro allows you to create a `Variant` from a single value or a list of values.
////// Usage:
/// - `v!(value)` creates a `Variant` from a single value.
/// - `v!(value1, value2, ...)` creates a `Vec<Variant>` from multiple values.
///
#[macro_export]
macro_rules! v {
  ($val:expr) => {
    Variant::from($val)
  };
  ($($element:expr),+) => {
    {
      let mut a = Vec::new();
      $(a.push(Variant::from($element));)*
      a
    }
  };
}

#[macro_export]
macro_rules! s {
  ($val:expr) => {
    Series::from($val)
  };
  ($($element:expr),+) => {
    {
      let mut a = Vec::new();
      $(a.push($element);)*
      Series::from(a)
    }
  };
}

macro_rules! getter {
  ($class:ident, $name:ident, $kind:ident, $type:ty) => {
    pub fn $name(&self) -> Option<&$type> {
      if let $class::$kind(v) = self {
        Some(v)
      } else {
        None
      }
    }
  };
}

macro_rules! getter_mut {
  ($class:ident, $name:ident, $kind:ident, $type:ty) => {
    pub fn $name(&mut self) -> Option<&mut $type> {
      if let $class::$kind(v) = self {
        Some(v)
      } else {
        None
      }
    }
  };
}
