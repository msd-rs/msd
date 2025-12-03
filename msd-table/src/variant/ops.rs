use super::Variant;
use std::ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Sub, SubAssign};

macro_rules! impl_operators {
  ($trait:ident, $method:ident, $trait_assign:ident, $method_assign:ident, $op:tt) => {
    impl $trait for Variant {
        type Output = Variant;
        fn $method(self, other: Self) -> Self::Output {
          if let Some(other) = other.cast(self.data_type()) {
            match (&self, other) {
              (Variant::Int32(a), Variant::Int32(b)) => Variant::Int32(a $op b),
              (Variant::Int64(a), Variant::Int64(b)) => Variant::Int64(a $op b),
              (Variant::Float32(a), Variant::Float32(b)) => Variant::Float32(a $op b),
              (Variant::Float64(a), Variant::Float64(b)) => Variant::Float64(a $op b),
              (Variant::Decimal64(a), Variant::Decimal64(b)) => Variant::Decimal64(*a $op b),
              (Variant::Decimal128(a), Variant::Decimal128(b)) => Variant::Decimal128(a $op b),
              _ => self
            }
          } else {
            self
          }
        }
    }

    impl $trait<&Variant> for Variant {
      type Output = Self;
      fn $method(self, other: &Variant) -> Self::Output {
        if let Some(other) = other.cast(self.data_type()) {
          match (&self, other) {
            (Variant::Int32(a), Variant::Int32(b)) => Variant::Int32(a $op b),
            (Variant::Int64(a), Variant::Int64(b)) => Variant::Int64(a $op b),
            (Variant::Float32(a), Variant::Float32(b)) => Variant::Float32(a $op b),
            (Variant::Float64(a), Variant::Float64(b)) => Variant::Float64(a $op b),
            (Variant::Decimal64(a), Variant::Decimal64(b)) => Variant::Decimal64(*a $op b),
            (Variant::Decimal128(a), Variant::Decimal128(b)) => Variant::Decimal128(a $op b),
            _ => self
          }
        } else {
          self
        }
      }
    }

    impl<'a, 'b> $trait<&'b Variant> for &'a Variant {
      type Output = Variant;
      fn $method(self, other: &'b Variant) -> Self::Output {
        if let Some(other) = other.cast(self.data_type()) {
          match (self, other) {
            (Variant::Int32(a), Variant::Int32(b)) => Variant::Int32(a $op b),
            (Variant::Int64(a), Variant::Int64(b)) => Variant::Int64(a $op b),
            (Variant::Float32(a), Variant::Float32(b)) => Variant::Float32(a $op b),
            (Variant::Float64(a), Variant::Float64(b)) => Variant::Float64(a $op b),
            (Variant::Decimal64(a), Variant::Decimal64(b)) => Variant::Decimal64(*a $op b),
            (Variant::Decimal128(a), Variant::Decimal128(b)) => Variant::Decimal128(a $op b),
            _ => self.clone()
          }
        } else {
          self.clone()
        }
      }
    }

    impl<'a, 'b> $trait_assign<&'b Variant> for Variant {
      fn $method_assign(&mut self, other: &'b Variant) {
        if let Some(other) = other.cast(self.data_type()) {
          match (self, other) {
            (Variant::Int32(a), Variant::Int32(b)) => *a = *a $op b,
            (Variant::Int64(a), Variant::Int64(b)) => *a = *a $op b,
            (Variant::Float32(a), Variant::Float32(b)) => *a = *a $op b,
            (Variant::Float64(a), Variant::Float64(b)) => *a = *a $op b,
            (Variant::Decimal64(a), Variant::Decimal64(b)) => *a = *a $op b,
            (Variant::Decimal128(a), Variant::Decimal128(b)) => *a = *a $op b,
            _ => {}
          }
        }
      }
    }




    impl $trait<usize> for Variant {
      type Output = Self;
      fn $method(self, other: usize) -> Self::Output {
        self $op Variant::from(other)
      }
    }

    impl $trait<f64> for Variant {
      type Output = Self;
      fn $method(self, other: f64) -> Self::Output {
        self $op Variant::from(other)
      }
    }
  };
  ($trait:ident, $method:ident, $trait_assign:ident, $method_assign:ident,$op:tt, $str_only:ident) => {
    impl $trait for Variant {
      type Output = Self;
      fn $method(self, other: Self) -> Self::Output {

        if let Some(other) = other.cast(self.data_type()) {
          match (&self, other) {
            (Variant::Int32(a), Variant::Int32(b)) => Variant::Int32(a $op b),
            (Variant::Int64(a), Variant::Int64(b)) => Variant::Int64(a $op b),
            (Variant::Float32(a), Variant::Float32(b)) => Variant::Float32(a $op b),
            (Variant::Float64(a), Variant::Float64(b)) => Variant::Float64(a $op b),
            (Variant::Decimal64(a), Variant::Decimal64(b)) => Variant::Decimal64(*a $op b),
            (Variant::Decimal128(a), Variant::Decimal128(b)) => Variant::Decimal128(a $op b),
            (Variant::String(a), Variant::String(b)) => Variant::String(a.to_owned() $op b.as_str()),
            _ => self
          }
        } else {
          self
        }
      }
    }

    impl $trait<&Self> for Variant {
      type Output = Self;
      fn $method(self, other: &Self) -> Self::Output {
        if let Some(other) = other.cast(self.data_type()) {
          match (&self, other) {
            (Variant::Int32(a), Variant::Int32(b)) => Variant::Int32(a $op b),
            (Variant::Int64(a), Variant::Int64(b)) => Variant::Int64(a $op b),
            (Variant::Float32(a), Variant::Float32(b)) => Variant::Float32(a $op b),
            (Variant::Float64(a), Variant::Float64(b)) => Variant::Float64(a $op b),
            (Variant::Decimal64(a), Variant::Decimal64(b)) => Variant::Decimal64(*a $op b),
            (Variant::Decimal128(a), Variant::Decimal128(b)) => Variant::Decimal128(a $op b),
            (Variant::String(a), Variant::String(b)) => Variant::String(a.to_owned() $op b.as_str()),
            _ => self
          }
        } else {
          self
        }
      }
    }

    impl<'a, 'b> $trait_assign<&'b Variant> for Variant {
      fn $method_assign(&mut self, other: &'b Variant) {
        if let Some(other) = other.cast(self.data_type()) {
          match (self, other) {
            (Variant::Int32(a), Variant::Int32(b)) => *a = *a $op b,
            (Variant::Int64(a), Variant::Int64(b)) => *a = *a $op b,
            (Variant::Float32(a), Variant::Float32(b)) => *a = *a $op b,
            (Variant::Float64(a), Variant::Float64(b)) => *a = *a $op b,
            (Variant::Decimal64(a), Variant::Decimal64(b)) => *a = *a $op b,
            (Variant::Decimal128(a), Variant::Decimal128(b)) => *a = *a $op b,
            _ => {}
          }
        }
      }
    }



    impl $trait<usize> for Variant {
      type Output = Self;
      fn $method(self, other: usize) -> Self::Output {
        self $op Variant::from(other)
      }
    }

    impl $trait<f64> for Variant {
      type Output = Self;
      fn $method(self, other: f64) -> Self::Output {
        self $op Variant::from(other)
      }
    }
  };
}

impl_operators!(Add, add, AddAssign, add_assign, +, true);
impl_operators!(Sub, sub, SubAssign, sub_assign, -);
impl_operators!(Mul, mul, MulAssign, mul_assign, *);
impl_operators!(Div, div, DivAssign, div_assign, /);
