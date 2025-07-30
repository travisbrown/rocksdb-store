use mapper::TableMapper;
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, OptimisticTransactionDB, Options, TransactionDB,
    TransactionDBOptions, DB,
};

use std::path::Path;

pub mod error;
pub mod mapper;
pub mod wrapper;

use error::Error;
use wrapper::Db;

const CONFIG_CF_NAME: &str = "_config";
const BOOKS_CF_NAME: &str = "_books";

type ConfigBincodeConfigType = bincode::config::Configuration<bincode::config::BigEndian>;
type BooksBincodeConfigType = bincode::config::Configuration<bincode::config::BigEndian>;

const CONFIG_BINCODE_CONFIG: ConfigBincodeConfigType =
    bincode::config::standard().with_big_endian();
const BOOKS_BINCODE_CONFIG: BooksBincodeConfigType = bincode::config::standard().with_big_endian();

#[derive(Clone)]
pub struct Database<const W: bool, C, B> {
    pub db: Db,
    pub config: C,
    pub books: B,
}

impl<const W: bool, C, B> Database<W, C, B> {
    fn config_cf(db: &Db) -> &ColumnFamily {
        db.handle(CONFIG_CF_NAME)
            .expect("Config table column family does not exist")
    }

    fn books_cf(db: &Db) -> &ColumnFamily {
        db.handle(BOOKS_CF_NAME)
            .expect("Books table column family does not exist")
    }

    fn config_mapper(db: &Db) -> TableMapper<'_, W, ConfigBincodeConfigType> {
        mapper::TableMapper::new(db, Self::config_cf(db), CONFIG_BINCODE_CONFIG)
    }

    fn books_mapper(db: &Db) -> TableMapper<'_, W, BooksBincodeConfigType> {
        mapper::TableMapper::new(db, Self::books_cf(db), BOOKS_BINCODE_CONFIG)
    }
}

impl<C: serde::ser::Serialize, B: serde::ser::Serialize> Database<true, C, B> {
    pub fn create<P: AsRef<Path>>(
        path: P,
        mut cfs: Vec<ColumnFamilyDescriptor>,
        mut options: Options,
        optimistic_transactions: bool,
        config: C,
        books: B,
    ) -> Result<Self, Error> {
        let config_cf = ColumnFamilyDescriptor::new(CONFIG_CF_NAME, Options::default());
        let books_cf = ColumnFamilyDescriptor::new(BOOKS_CF_NAME, Options::default());

        cfs.push(config_cf);
        cfs.push(books_cf);

        options.create_missing_column_families(true);
        options.create_if_missing(true);

        let db: Db = if optimistic_transactions {
            OptimisticTransactionDB::open_cf_descriptors(&options, path, cfs)?.into()
        } else {
            let transaction_options = TransactionDBOptions::default();

            TransactionDB::open_cf_descriptors(&options, &transaction_options, path, cfs)?.into()
        };

        Self::write_config_with_db(&db, &config)?;
        Self::write_books_with_db(&db, &books)?;

        Ok(Self { db, config, books })
    }

    pub fn write_config(&self, config: &C) -> Result<(), mapper::Error> {
        Self::write_config_with_db(&self.db, config)
    }

    pub fn write_books(&self, books: &B) -> Result<(), mapper::Error> {
        Self::write_books_with_db(&self.db, books)
    }

    fn write_config_with_db(db: &Db, config: &C) -> Result<(), mapper::Error> {
        config.serialize(Self::config_mapper(db))
    }

    fn write_books_with_db(db: &Db, books: &B) -> Result<(), mapper::Error> {
        books.serialize(Self::books_mapper(db))
    }
}

impl<'de, C: serde::de::Deserialize<'de>, B: serde::de::Deserialize<'de>> Database<true, C, B> {
    pub fn open_with_pessimistic_transactions<P: AsRef<Path>>(
        path: P,
        cfs: Vec<ColumnFamilyDescriptor>,
        options: Options,
    ) -> Result<Self, Error> {
        Database::open_internal(path, cfs, options, true)
    }
}

impl<'de, const W: bool, C: serde::de::Deserialize<'de>, B: serde::de::Deserialize<'de>>
    Database<W, C, B>
{
    pub fn open<P: AsRef<Path>>(
        path: P,
        cfs: Vec<ColumnFamilyDescriptor>,
        options: Options,
    ) -> Result<Self, Error> {
        Self::open_internal(path, cfs, options, true)
    }

    pub fn read_config(&self) -> Result<C, mapper::Error> {
        Self::read_config_with_db(&self.db)
    }

    pub fn read_books(&self) -> Result<B, mapper::Error> {
        Self::read_books_with_db(&self.db)
    }

    fn open_internal<P: AsRef<Path>>(
        path: P,
        mut cfs: Vec<ColumnFamilyDescriptor>,
        options: Options,
        optimistic_transactions: bool,
    ) -> Result<Self, Error> {
        let config_cf = ColumnFamilyDescriptor::new(CONFIG_CF_NAME, Options::default());
        let books_cf = ColumnFamilyDescriptor::new(BOOKS_CF_NAME, Options::default());

        cfs.push(config_cf);
        cfs.push(books_cf);

        let db: Db = if !W {
            DB::open_cf_descriptors_read_only(&options, path, cfs, false)?.into()
        } else if optimistic_transactions {
            OptimisticTransactionDB::open_cf_descriptors(&options, path, cfs)?.into()
        } else {
            let transaction_options = TransactionDBOptions::default();

            TransactionDB::open_cf_descriptors(&options, &transaction_options, path, cfs)?.into()
        };

        let config = Self::read_config_with_db(&db)?;
        let books = Self::read_books_with_db(&db)?;

        Ok(Self { db, config, books })
    }

    fn read_config_with_db(db: &Db) -> Result<C, mapper::Error> {
        C::deserialize(&Self::config_mapper(db))
    }

    fn read_books_with_db(db: &Db) -> Result<B, mapper::Error> {
        B::deserialize(&Self::books_mapper(db))
    }
}

impl<C, B> Database<true, C, B> {
    pub fn admin<P: AsRef<Path>>(
        path: P,
        mut cfs: Vec<ColumnFamilyDescriptor>,
    ) -> Result<Admin, Error> {
        let config_cf = ColumnFamilyDescriptor::new(CONFIG_CF_NAME, Options::default());
        let books_cf = ColumnFamilyDescriptor::new(BOOKS_CF_NAME, Options::default());

        cfs.push(config_cf);
        cfs.push(books_cf);

        let cf_names = cfs.iter().map(|cf| cf.name().to_string()).collect();

        Ok(Admin {
            underlying: DB::open_cf_descriptors(&Options::default(), path, cfs)?,
            cf_names,
        })
    }
}

impl<C, B> Database<false, C, B> {
    pub fn underlying(&self) -> &DB {
        // Safe because we know statically that the database is read-only.
        self.db.read_only().unwrap()
    }
}

pub struct Admin {
    underlying: DB,
    cf_names: Vec<String>,
}

impl Admin {
    pub fn flush(&self) -> Result<(), rocksdb::Error> {
        for cf_name in &self.cf_names {
            if let Some(cf) = self.underlying.cf_handle(cf_name) {
                self.underlying.flush_cf(cf)?;
            }
        }

        Ok(())
    }

    pub fn compact(&self) -> Result<(), rocksdb::Error> {
        let mut options = rocksdb::CompactOptions::default();
        options.set_change_level(true);

        for cf_name in &self.cf_names {
            if let Some(cf) = self.underlying.cf_handle(cf_name) {
                self.underlying
                    .compact_range_cf_opt::<&[u8], &[u8]>(cf, None, None, &options);
            }
        }

        self.underlying.wait_for_compact(&Default::default())
    }
}

#[cfg(test)]
mod tests {
    use quickcheck_arbitrary_derive::QuickCheck;

    #[derive(
        Clone,
        Copy,
        Debug,
        Default,
        Eq,
        PartialEq,
        QuickCheck,
        serde_derive::Deserialize,
        serde_derive::Serialize,
    )]
    pub enum Hashes {
        #[default]
        Both,
        Md5Only,
        Sha256Only,
    }

    #[derive(
        Clone, Debug, Eq, PartialEq, QuickCheck, serde_derive::Deserialize, serde_derive::Serialize,
    )]
    struct Config {
        hashes: Hashes,
        case_sensitive: bool,
    }

    #[derive(
        Clone, Debug, Eq, PartialEq, QuickCheck, serde_derive::Deserialize, serde_derive::Serialize,
    )]
    struct Books {
        last_scrape_ms: u64,
        region: String,
    }

    #[quickcheck_macros::quickcheck]
    fn round_trip_instantiate(config: Config, books: Books) -> bool {
        let test_db_dir = tempfile::tempdir().unwrap();

        let writeable_db = super::Database::create(
            &test_db_dir,
            vec![],
            Default::default(),
            true,
            config.clone(),
            books.clone(),
        )
        .unwrap();

        writeable_db.db.close();

        let read_only_db =
            super::Database::<true, Config, Books>::open(test_db_dir, vec![], Default::default())
                .unwrap();

        read_only_db.config == config && read_only_db.books == books
    }

    #[quickcheck_macros::quickcheck]
    fn round_trip_instantiate_with_unit(config: (), books: ()) -> bool {
        let test_db_dir = tempfile::tempdir().unwrap();

        let writeable_db =
            super::Database::create(&test_db_dir, vec![], Default::default(), true, (), ())
                .unwrap();

        writeable_db.db.close();

        let read_only_db =
            super::Database::<true, (), ()>::open(test_db_dir, vec![], Default::default()).unwrap();

        read_only_db.config == config && read_only_db.books == books
    }

    #[quickcheck_macros::quickcheck]
    fn round_trip_instantiate_with_pessimistic_transactions(config: Config, books: Books) -> bool {
        let test_db_dir = tempfile::tempdir().unwrap();

        let writeable_db = super::Database::create(
            &test_db_dir,
            vec![],
            Default::default(),
            false,
            config.clone(),
            books.clone(),
        )
        .unwrap();

        writeable_db.db.close();

        let read_only_db =
            super::Database::<true, Config, Books>::open(test_db_dir, vec![], Default::default())
                .unwrap();

        read_only_db.config == config && read_only_db.books == books
    }

    #[quickcheck_macros::quickcheck]
    fn round_trip_write(
        config: Config,
        books: Books,
        new_config: Config,
        new_books: Books,
    ) -> bool {
        let test_db_dir = tempfile::tempdir().unwrap();

        let writeable_db = super::Database::create(
            &test_db_dir,
            vec![],
            Default::default(),
            true,
            config.clone(),
            books.clone(),
        )
        .unwrap();

        writeable_db.db.close();

        let writeable_db =
            super::Database::<true, Config, Books>::open(&test_db_dir, vec![], Default::default())
                .unwrap();

        assert!(writeable_db.config == config && writeable_db.books == books);

        writeable_db.write_config(&new_config).unwrap();
        writeable_db.write_books(&new_books).unwrap();

        writeable_db.read_config().unwrap() == new_config
            && writeable_db.read_books().unwrap() == new_books
    }

    #[quickcheck_macros::quickcheck]
    fn round_trip_write_with_pessimistic_transactions(
        config: Config,
        books: Books,
        new_config: Config,
        new_books: Books,
    ) -> bool {
        let test_db_dir = tempfile::tempdir().unwrap();

        let writeable_db = super::Database::create(
            &test_db_dir,
            vec![],
            Default::default(),
            false,
            config.clone(),
            books.clone(),
        )
        .unwrap();

        writeable_db.db.close();

        let writeable_db =
            super::Database::<true, Config, Books>::open_with_pessimistic_transactions(
                &test_db_dir,
                vec![],
                Default::default(),
            )
            .unwrap();

        assert!(writeable_db.config == config && writeable_db.books == books);

        writeable_db.write_config(&new_config).unwrap();
        writeable_db.write_books(&new_books).unwrap();

        writeable_db.read_config().unwrap() == new_config
            && writeable_db.read_books().unwrap() == new_books
    }
}
