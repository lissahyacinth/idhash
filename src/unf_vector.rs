use arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, UInt16Array,
    UInt32Array, UInt64Array, Utf8Array,
};

const NULL_STRING: &str = "null";

/// Convertible to a Univerally Comparable Vector
pub trait UNFVector {
    /// Provide a Vector of Bytes for each Column
    fn raw<'a>(
        &'a self,
        characters: usize,
        digits: usize,
        has_nulls: bool,
    ) -> Box<dyn Iterator<Item = Vec<u8>> + 'a> {
        let unf_digits = self.to_unf(digits, has_nulls);
        Box::new(unf_digits.into_iter().map(move |x| {
            let mut encoded_string: Vec<u8> = Vec::with_capacity(characters + 4);
            for item in x.chars().take(characters) {
                encoded_string.push(item as u8);
            }
            encoded_string.push(b'\n');
            encoded_string.push(b'\x00');
            encoded_string.into_iter().map(|x| x as u8).collect()
        }))
    }
    fn to_unf<'a>(
        &'a self,
        _digits: usize,
        has_nulls: bool,
    ) -> Box<dyn Iterator<Item = String> + 'a>;
}

pub fn sigfig(value: f64, digits: usize) -> usize {
    match value {
        v if v <= 0_f64 => digits,
        _ => digits.saturating_sub(value.abs().log10() as usize + 1),
    }
}

impl UNFVector for Float64Array {
    fn to_unf<'a>(
        &'a self,
        digits: usize,
        has_null: bool,
    ) -> Box<dyn Iterator<Item = String> + 'a> {
        if has_null {
            Box::new(self.into_iter().map(move |x| match x {
                Some(val) => format!("{:e}", sigfig(*val, digits - 1)),
                None => String::from(NULL_STRING),
            }))
        } else {
            Box::new(
                self.into_iter()
                    .map(move |x| format!("{:e}", (sigfig(*x.unwrap(), digits - 1)))),
            )
        }
    }
}

impl UNFVector for Float32Array {
    fn to_unf<'a>(
        &'a self,
        digits: usize,
        has_null: bool,
    ) -> Box<dyn Iterator<Item = String> + 'a> {
        if has_null {
            Box::new(self.into_iter().map(move |x| match x {
                Some(val) => format!("{:e}", (sigfig(*val as f64, digits - 1))),
                None => String::from(NULL_STRING),
            }))
        } else {
            Box::new(
                self.into_iter()
                    .map(move |x| format!("{:e}", sigfig(*x.unwrap() as f64, digits - 1))),
            )
        }
    }
}

macro_rules! integer_unf {
    ($array_type: ident) => {
        impl UNFVector for $array_type {
            #[inline(always)]
            fn to_unf<'a>(
                &'a self,
                _digits: usize,
                has_null: bool,
            ) -> Box<dyn Iterator<Item = String> + 'a> {
                if has_null {
                    Box::new(self.into_iter().map(|x| match x {
                        Some(val) => val.to_string(),
                        None => String::from(NULL_STRING),
                    }))
                } else {
                    Box::new(self.into_iter().map(|x| x.unwrap().to_string()))
                }
            }
        }
    };
}

integer_unf!(Int16Array);
integer_unf!(Int32Array);
integer_unf!(Int64Array);

integer_unf!(UInt16Array);
integer_unf!(UInt32Array);
integer_unf!(UInt64Array);

impl UNFVector for Utf8Array<i32> {
    fn to_unf<'a>(
        &'a self,
        _digits: usize,
        has_null: bool,
    ) -> Box<dyn Iterator<Item = String> + 'a> {
        if has_null {
            Box::new(self.into_iter().map(|x| match x {
                Some(val) => String::from(val),
                None => String::from(NULL_STRING),
            }))
        } else {
            Box::new(self.into_iter().map(|x| String::from(x.unwrap())))
        }
    }
}

impl UNFVector for Utf8Array<i64> {
    fn to_unf<'a>(
        &'a self,
        _digits: usize,
        has_null: bool,
    ) -> Box<dyn Iterator<Item = String> + 'a> {
        if has_null {
            Box::new(self.into_iter().map(|x| match x {
                Some(val) => String::from(val),
                None => String::from(NULL_STRING),
            }))
        } else {
            Box::new(self.into_iter().map(|x| String::from(x.unwrap())))
        }
    }
}

impl UNFVector for BooleanArray {
    fn to_unf<'a>(
        &'a self,
        _digits: usize,
        has_null: bool,
    ) -> Box<dyn Iterator<Item = String> + 'a> {
        if has_null {
            Box::new(self.into_iter().map(|x| match x {
                Some(val) => match val {
                    true => String::from("true"),
                    false => String::from("false"),
                },
                None => String::from(NULL_STRING),
            }))
        } else {
            Box::new(self.into_iter().map(|x| match x.unwrap() {
                true => String::from("true"),
                false => String::from("false"),
            }))
        }
    }
}
