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

mod datatype;
mod field;
mod series;
mod table;
mod variant;

pub use datatype::DataType;
pub use field::Field;
pub use series::Series;
pub use table::{Table, TableColumn};
pub use variant::{Variant, VariantMutRef, VariantRef};
