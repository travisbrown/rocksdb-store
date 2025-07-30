use rocksdb::{
    ColumnFamily, DBPinnableSlice, IteratorMode, OptimisticTransactionDB, TransactionDB, DB,
};
use std::sync::Arc;

type KeyValuePair = (Box<[u8]>, Box<[u8]>);

enum DbInner {
    ReadOnly(DB),
    OptimisticTransaction(OptimisticTransactionDB),
    PessimisticTransaction(TransactionDB),
}

/// Simple abstraction over read-only and transactional writeable databases.
#[derive(Clone)]
pub struct Db(Arc<DbInner>);

impl From<DB> for Db {
    fn from(value: DB) -> Self {
        Self(Arc::new(DbInner::ReadOnly(value)))
    }
}

impl From<OptimisticTransactionDB> for Db {
    fn from(value: OptimisticTransactionDB) -> Self {
        Self(Arc::new(DbInner::OptimisticTransaction(value)))
    }
}

impl From<TransactionDB> for Db {
    fn from(value: TransactionDB) -> Self {
        Self(Arc::new(DbInner::PessimisticTransaction(value)))
    }
}

impl Db {
    // For internal use only.
    pub(super) fn read_only(&self) -> Option<&DB> {
        match self.0.as_ref() {
            DbInner::ReadOnly(inner) => Some(inner),
            _ => None,
        }
    }

    pub fn transaction(&self) -> Option<Transaction<'_>> {
        match self.0.as_ref() {
            DbInner::ReadOnly(_) => None,
            DbInner::OptimisticTransaction(db) => Some(Transaction::Optimistic(db.transaction())),
            DbInner::PessimisticTransaction(db) => Some(Transaction::Pessimistic(db.transaction())),
        }
    }

    pub fn handle(&self, name: &str) -> Option<&ColumnFamily> {
        match self.0.as_ref() {
            DbInner::ReadOnly(db) => db.cf_handle(name),
            DbInner::OptimisticTransaction(db) => db.cf_handle(name),
            DbInner::PessimisticTransaction(db) => db.cf_handle(name),
        }
    }

    pub fn get<K: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        key: K,
    ) -> Result<Option<DBPinnableSlice<'_>>, rocksdb::Error> {
        match self.0.as_ref() {
            DbInner::ReadOnly(db) => db.get_pinned_cf(cf, key),
            DbInner::OptimisticTransaction(db) => db.get_pinned_cf(cf, key),
            DbInner::PessimisticTransaction(db) => db.get_pinned_cf(cf, key),
        }
    }

    pub fn multi_get<K: AsRef<[u8]>, I: IntoIterator<Item = K>>(
        &self,
        cf: &ColumnFamily,
        keys: I,
    ) -> Result<Vec<Option<Vec<u8>>>, rocksdb::Error> {
        match self.0.as_ref() {
            DbInner::ReadOnly(db) => db.multi_get_cf(keys.into_iter().map(|key| (cf, key))),
            DbInner::OptimisticTransaction(db) => {
                db.multi_get_cf(keys.into_iter().map(|key| (cf, key)))
            }
            DbInner::PessimisticTransaction(db) => {
                db.multi_get_cf(keys.into_iter().map(|key| (cf, key)))
            }
        }
        .into_iter()
        .collect()
    }

    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        key: K,
        value: V,
    ) -> Result<(), rocksdb::Error> {
        match self.0.as_ref() {
            DbInner::ReadOnly(db) => db.put_cf(cf, key, value),
            DbInner::OptimisticTransaction(db) => db.put_cf(cf, key, value),
            DbInner::PessimisticTransaction(db) => db.put_cf(cf, key, value),
        }
    }

    pub fn merge<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        key: K,
        value: V,
    ) -> Result<(), rocksdb::Error> {
        match self.0.as_ref() {
            DbInner::ReadOnly(db) => db.merge_cf(cf, key, value),
            DbInner::OptimisticTransaction(db) => db.merge_cf(cf, key, value),
            DbInner::PessimisticTransaction(db) => db.merge_cf(cf, key, value),
        }
    }

    pub fn iterator(
        &self,
        cf: &ColumnFamily,
        mode: IteratorMode,
    ) -> impl Iterator<Item = Result<KeyValuePair, rocksdb::Error>> + use<'_> {
        let iterator: Box<dyn Iterator<Item = Result<KeyValuePair, rocksdb::Error>>> =
            match self.0.as_ref() {
                DbInner::ReadOnly(db) => Box::new(db.iterator_cf(cf, mode)),
                DbInner::OptimisticTransaction(db) => Box::new(db.iterator_cf(cf, mode)),
                DbInner::PessimisticTransaction(db) => Box::new(db.iterator_cf(cf, mode)),
            };

        iterator
    }

    pub fn close(self) {
        std::mem::drop(self)
    }
}

/// Simple abstraction over transaction type (optimistic or pessmistic).
pub enum Transaction<'a> {
    Optimistic(rocksdb::Transaction<'a, OptimisticTransactionDB>),
    Pessimistic(rocksdb::Transaction<'a, TransactionDB>),
}

impl<'a> Transaction<'a> {
    pub fn commit(self) -> Result<(), rocksdb::Error> {
        match self {
            Self::Optimistic(tx) => tx.commit(),
            Self::Pessimistic(tx) => tx.commit(),
        }
    }

    pub fn get<K: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        key: K,
    ) -> Result<Option<DBPinnableSlice<'_>>, rocksdb::Error> {
        match self {
            Self::Optimistic(tx) => tx.get_pinned_cf(cf, key),
            Self::Pessimistic(tx) => tx.get_pinned_cf(cf, key),
        }
    }

    pub fn multi_get<K: AsRef<[u8]>, I: IntoIterator<Item = K>>(
        &self,
        cf: &ColumnFamily,
        keys: I,
    ) -> Result<Vec<Option<Vec<u8>>>, rocksdb::Error> {
        match self {
            Self::Optimistic(tx) => tx.multi_get_cf(keys.into_iter().map(|key| (cf, key))),
            Self::Pessimistic(tx) => tx.multi_get_cf(keys.into_iter().map(|key| (cf, key))),
        }
        .into_iter()
        .collect()
    }

    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        key: K,
        value: V,
    ) -> Result<(), rocksdb::Error> {
        match self {
            Self::Optimistic(tx) => tx.put_cf(cf, key, value),
            Self::Pessimistic(tx) => tx.put_cf(cf, key, value),
        }
    }

    pub fn merge<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        key: K,
        value: V,
    ) -> Result<(), rocksdb::Error> {
        match self {
            Self::Optimistic(tx) => tx.merge_cf(cf, key, value),
            Self::Pessimistic(tx) => tx.merge_cf(cf, key, value),
        }
    }

    pub fn iterator(
        &self,
        cf: &ColumnFamily,
        mode: IteratorMode,
    ) -> impl Iterator<Item = Result<KeyValuePair, rocksdb::Error>> + use<'_> {
        let iterator: Box<dyn Iterator<Item = Result<KeyValuePair, rocksdb::Error>>> = match self {
            Self::Optimistic(tx) => Box::new(tx.iterator_cf(cf, mode)),
            Self::Pessimistic(tx) => Box::new(tx.iterator_cf(cf, mode)),
        };

        iterator
    }
}
