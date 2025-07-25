use crate::wrapper::Transaction;

use super::wrapper::Db;
use bincode::serde::OwnedSerdeDecoder;
use rocksdb::ColumnFamily;
use std::io::{BufReader, Cursor};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Unsupported configuration type")]
    Unsupported,
    #[error("Invalid RocksDB transaction state")]
    InvalidTransaction,
    #[error("Encoding error")]
    Encoding(#[from] bincode::error::EncodeError),
    #[error("Decoding error")]
    Decoding(bincode::error::DecodeError),
    #[error("Serde error")]
    Serde(serde::de::value::Error),
    #[error("RocksDb error")]
    Db(#[from] rocksdb::Error),
}

impl serde::ser::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self::Serde(serde::de::value::Error::custom(msg))
    }
}

impl serde::de::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self::Serde(serde::de::value::Error::custom(msg))
    }

    fn duplicate_field(field: &'static str) -> Self {
        Self::Serde(serde::de::value::Error::duplicate_field(field))
    }

    fn invalid_length(len: usize, exp: &dyn serde::de::Expected) -> Self {
        Self::Serde(serde::de::value::Error::invalid_length(len, exp))
    }

    fn invalid_type(unexp: serde::de::Unexpected, exp: &dyn serde::de::Expected) -> Self {
        Self::Serde(serde::de::value::Error::invalid_type(unexp, exp))
    }

    fn invalid_value(unexp: serde::de::Unexpected, exp: &dyn serde::de::Expected) -> Self {
        Self::Serde(serde::de::value::Error::invalid_value(unexp, exp))
    }

    fn missing_field(field: &'static str) -> Self {
        Self::Serde(serde::de::value::Error::missing_field(field))
    }

    fn unknown_field(field: &str, expected: &'static [&'static str]) -> Self {
        Self::Serde(serde::de::value::Error::unknown_field(field, expected))
    }

    fn unknown_variant(variant: &str, expected: &'static [&'static str]) -> Self {
        Self::Serde(serde::de::value::Error::unknown_variant(variant, expected))
    }
}

/// Maps a serializable struct onto a column family.
pub struct TableMapper<'a, const W: bool, C> {
    db: &'a Db,
    tx: Option<Transaction<'a>>,
    cf: &'a ColumnFamily,
    bincode_config: C,
}

impl<'a, const W: bool, C> TableMapper<'a, W, C> {
    pub(super) fn new(db: &'a Db, cf: &'a ColumnFamily, bincode_config: C) -> Self {
        Self {
            db,
            tx: if W {
                // Safe because we know the wrapper is writeable.
                Some(db.transaction().unwrap())
            } else {
                None
            },
            cf,
            bincode_config,
        }
    }
}

impl<'a, C: bincode::config::Config> serde::ser::SerializeStruct for TableMapper<'a, true, C> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + serde::Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        let value_bytes = bincode::serde::encode_to_vec(value, self.bincode_config)?;

        self.tx
            .as_ref()
            .ok_or(Error::InvalidTransaction)
            .and_then(|tx| {
                tx.put(self.cf, key.as_bytes(), value_bytes)
                    .map_err(Error::from)
            })
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        self.tx
            .take()
            .ok_or(Error::InvalidTransaction)
            .and_then(|tx| tx.commit().map_err(Error::from))
    }
}

impl<'a, C: bincode::config::Config> serde::ser::Serializer for TableMapper<'a, true, C> {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_i128(self, _v: i128) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_newtype_struct<T: ?Sized + serde::Serialize>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_newtype_variant<T: ?Sized + serde::Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_some<T: ?Sized + serde::Serialize>(
        self,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_u128(self, _v: u128) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }
}

impl<'a, C> serde::ser::SerializeMap for TableMapper<'a, true, C> {
    type Ok = ();
    type Error = Error;

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_entry<K: ?Sized + serde::Serialize, V: ?Sized + serde::Serialize>(
        &mut self,
        _key: &K,
        _value: &V,
    ) -> Result<(), Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_key<T: ?Sized + serde::Serialize>(&mut self, _key: &T) -> Result<(), Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_value<T: ?Sized + serde::Serialize>(
        &mut self,
        _value: &T,
    ) -> Result<(), Self::Error> {
        Err(Error::Unsupported)
    }
}

impl<'a, C> serde::ser::SerializeSeq for TableMapper<'a, true, C> {
    type Ok = ();
    type Error = Error;

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_element<T: ?Sized + serde::Serialize>(
        &mut self,
        _value: &T,
    ) -> Result<(), Self::Error> {
        Err(Error::Unsupported)
    }
}

impl<'a, C> serde::ser::SerializeStructVariant for TableMapper<'a, true, C> {
    type Ok = ();
    type Error = Error;

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_field<T: ?Sized + serde::Serialize>(
        &mut self,
        _key: &'static str,
        _value: &T,
    ) -> Result<(), Self::Error> {
        Err(Error::Unsupported)
    }

    fn skip_field(&mut self, _key: &'static str) -> Result<(), Self::Error> {
        Err(Error::Unsupported)
    }
}

impl<'a, C> serde::ser::SerializeTuple for TableMapper<'a, true, C> {
    type Ok = ();
    type Error = Error;

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_element<T: ?Sized + serde::Serialize>(
        &mut self,
        _value: &T,
    ) -> Result<(), Self::Error> {
        Err(Error::Unsupported)
    }
}

impl<'a, C> serde::ser::SerializeTupleStruct for TableMapper<'a, true, C> {
    type Ok = ();
    type Error = Error;

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_field<T: ?Sized + serde::Serialize>(
        &mut self,
        _value: &T,
    ) -> Result<(), Self::Error> {
        Err(Error::Unsupported)
    }
}

impl<'a, C> serde::ser::SerializeTupleVariant for TableMapper<'a, true, C> {
    type Ok = ();
    type Error = Error;

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::Unsupported)
    }

    fn serialize_field<T: ?Sized + serde::Serialize>(
        &mut self,
        _value: &T,
    ) -> Result<(), Self::Error> {
        Err(Error::Unsupported)
    }
}

impl<'a, 'de: 'a, const W: bool, C: bincode::config::Config> serde::de::Deserializer<'de>
    for &TableMapper<'a, W, C>
{
    type Error = Error;

    fn deserialize_struct<V: serde::de::Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_map(TableMapperAccess {
            table: self,
            fields,
        })
    }

    fn is_human_readable(&self) -> bool {
        false
    }

    fn deserialize_any<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_bool<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_byte_buf<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_bytes<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_char<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_enum<V: serde::de::Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_f32<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_f64<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_i16<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_i32<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_i64<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_i8<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_identifier<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_ignored_any<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_newtype_struct<V: serde::de::Visitor<'de>>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_map<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_option<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_seq<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_str<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_string<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_tuple<V: serde::de::Visitor<'de>>(
        self,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_tuple_struct<V: serde::de::Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_u16<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_u32<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_u64<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_u8<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported)
    }

    fn deserialize_unit<V: serde::de::Visitor<'de>>(
        self,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V: serde::de::Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_unit()
    }
}

struct TableMapperAccess<'a, const W: bool, C> {
    table: &'a TableMapper<'a, W, C>,
    fields: &'static [&'static str],
}

impl<'a, 'de: 'a, const W: bool, C: bincode::config::Config> serde::de::MapAccess<'de>
    for TableMapperAccess<'a, W, C>
{
    type Error = Error;

    fn next_key_seed<K: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        if self.fields.is_empty() {
            Ok(None)
        } else {
            let deserializer = serde::de::value::StrDeserializer::new(self.fields[0]);

            seed.deserialize(deserializer).map(Some)
        }
    }

    fn next_value_seed<V: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, Self::Error> {
        // In the case that the field is not found, we return the Bincode representation for `None`.
        const BINCODE_NONE_BYTES: [u8; 1] = [0];

        let field_name = self.fields[0].as_bytes();
        self.fields = &self.fields[1..];

        let bytes = self.table.db.get(self.table.cf, field_name)?;

        match bytes {
            Some(bytes) => {
                let mut deserializer = OwnedSerdeDecoder::from_reader(
                    BufReader::new(Cursor::new(bytes)),
                    self.table.bincode_config,
                );

                seed.deserialize(deserializer.as_deserializer())
                    .map_err(Error::Decoding)
            }
            None => {
                let mut deserializer = OwnedSerdeDecoder::from_reader(
                    BufReader::new(Cursor::new(BINCODE_NONE_BYTES)),
                    self.table.bincode_config,
                );

                seed.deserialize(deserializer.as_deserializer())
                    .map_err(Error::Decoding)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use quickcheck_arbitrary_derive::QuickCheck;
    use serde::{de::Deserialize, ser::Serialize};

    #[derive(
        Clone, Debug, Eq, PartialEq, QuickCheck, serde_derive::Deserialize, serde_derive::Serialize,
    )]
    struct Test {
        foo: String,
        bar: Vec<Option<u64>>,
        qux: bool,
    }

    #[quickcheck_macros::quickcheck]
    fn round_trip_test(test: Test, new_foo: String) -> bool {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let db = rocksdb::OptimisticTransactionDB::open_cf_descriptors(
            &options,
            tempfile::tempdir().unwrap(),
            vec![rocksdb::ColumnFamilyDescriptor::new(
                "test",
                rocksdb::Options::default(),
            )],
        )
        .unwrap();

        let wrapper = crate::wrapper::Db::from(db);

        let mapper = super::TableMapper::new(
            &wrapper,
            wrapper.handle("test").unwrap(),
            bincode::config::standard(),
        );

        test.serialize(mapper).unwrap();

        let mapper = super::TableMapper::<true, _>::new(
            &wrapper,
            wrapper.handle("test").unwrap(),
            bincode::config::standard(),
        );

        let read_test = Test::deserialize(&mapper).unwrap();

        let mut new_test = read_test.clone();
        new_test.foo = new_foo;

        let mapper = super::TableMapper::new(
            &wrapper,
            wrapper.handle("test").unwrap(),
            bincode::config::standard(),
        );

        new_test.serialize(mapper).unwrap();

        let mapper = super::TableMapper::<true, _>::new(
            &wrapper,
            wrapper.handle("test").unwrap(),
            bincode::config::standard(),
        );

        let new_read_test = Test::deserialize(&mapper).unwrap();

        read_test == test && new_read_test == new_test
    }
}
