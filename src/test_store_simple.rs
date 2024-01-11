

#[cfg(test)]
pub mod tests {
    use std::collections::HashSet;

    use std::marker::PhantomData;
    use std::sync::Arc;
    use scupt_util::node_id::NID;
    use async_trait::async_trait;

    use rusqlite::{Connection, params, Transaction};
    use scupt_util::error_type::ET;
    use scupt_util::message::MsgTrait;
    use scupt_util::mt_set::MTSet;

    use scupt_util::res::Res;
    use scupt_util::res_of::{res_option, res_sqlite};
    use serde::{Deserialize, Serialize};
    use serde::de::DeserializeOwned;
    use tracing::error;
    use uuid::Uuid;
    use crate::check_invariants::tests::InvariantChecker;


    use crate::raft_config::RaftConf;
    use crate::raft_message::LogEntry;

    use crate::sm_store::{SMStore, WriteEntriesOpt, WriteSnapshotOpt};
    use crate::snapshot::{IndexValue, Snapshot, SnapshotIndexTerm};


    struct StoreInner<T: MsgTrait + 'static> {
        p: PhantomData<T>,
        path: String,
        cluster_name:String,
    }

    pub struct SMStoreSimple<T: MsgTrait + 'static> {
        enable_check_invariants:bool,
        inner: Arc<StoreInner<T>>,
    }


    impl<T: MsgTrait + 'static> SMStoreSimple<T> {
        pub fn create(conf: RaftConf, enable_check_invariants:bool) -> Res<Self> {
            let inner = StoreInner::<T>::new(
                conf.storage_path.clone(),
                conf.cluster_name.clone())?;
            inner.write_config(conf)?;
            let s = Self {
                enable_check_invariants,
                inner: Arc::new(inner),
            };
            Ok(s)
        }


        pub async fn write_snapshot_gut(
            &self,
            snapshot: SnapshotIndexTerm,
            values: Vec<IndexValue<T>>,
            truncate_left: Option<u64>,
            truncate_right: Option<u64>,
            _option: Option<Vec<u8>>) -> Res<()> {
            let mut conn = self.inner.connection()?;
            let trans = res_sqlite(conn.transaction())?;
            self.inner.write_snapshot_gut(&trans, snapshot, values,
                                          truncate_left, truncate_right, _option)?;
            res_sqlite(trans.commit())?;
            Ok(())
        }
        async fn reset_all(&self) -> Res<()> {
            if !self.enable_check_invariants {
                return Ok(());
            }
            let (snapshot, log) = self.read_snapshot_and_log().await?;
            let (term, voted_for) = self.inner.get_term_voted()?;
            InvariantChecker::reset_committed(
                self.inner.cluster_name.clone()
            );
            InvariantChecker::set_and_check_invariants(
                self.inner.cluster_name.clone(),
                self.inner.path.clone(),
                Some(0),
                None,
                Some((term, voted_for)),
                Some((snapshot, log)),
                false
            );
            Ok(())
        }

        async fn read_snapshot_and_log(&self) -> Res<(Snapshot<T>, Vec<LogEntry<T>>)> {
            let (index_term, values, _) = self.read_snapshot(Uuid::new_v4().to_string(), None).await?;
            let snapshot = Snapshot {
                term: index_term.term,
                index: index_term.index,
                value: MTSet::new(values.into_iter().collect()),
            };
            let start = self.get_min_log_index().await?;
            let end = self.get_max_log_index().await? + 1;
            let log = self.read_log_entries(start, end).await?;
            Ok((snapshot, log))
        }

        async fn snapshot_log_changed_check(&self) -> Res<()> {
            if !self.enable_check_invariants {
                return Ok(());
            }

            let (snapshot, log) = self.read_snapshot_and_log().await?;

            InvariantChecker::set_and_check_invariants(
                self.inner.cluster_name.clone(),
                    self.inner.path.clone(),
                    None,
                    None,
                    None,
                    Some((snapshot, log)),
                    true
            );

            Ok(())
        }

        async fn changed_term_voted_check(&self) -> Res<()> {
            if !self.enable_check_invariants {
                return Ok(());
            }
            let (term, voted_for) = self.inner.get_term_voted()?;
            InvariantChecker::set_and_check_invariants::<T>(
                self.inner.cluster_name.clone(),
                self.inner.path.clone(),
                None,
                None,
                Some((term, voted_for)),
                None,
                true
            );
            Ok(())
        }

        async fn changed_commit_index_check(&self) -> Res<()>{
            if !self.enable_check_invariants {
                return Ok(());
            }
            let index = self.inner.get_commit_index()?;
            InvariantChecker::update_commit_index(
                self.inner.cluster_name.clone(),
                self.inner.path.clone(),
                index);

            Ok(())
        }
    }



    #[async_trait]
    impl<T: MsgTrait + 'static> SMStore<T> for SMStoreSimple<T> {
        /// update raft configuration

        async fn write_config(&self, conf: RaftConf) -> Res<()> {
            self.inner.write_config(conf)
        }

        ///
        async fn read_config(&self) -> Res<RaftConf> {
            self.inner.read_config()
        }

        /// update term and voted_for
        async fn set_term_voted(&self, term: u64, voted_for: Option<NID>) -> Res<()> {
            {
                let mut conn = self.inner.connection()?;
                let trans = res_sqlite(conn.transaction())?;
                self.inner.set_term_voted(&trans, term, voted_for)?;
                trans.commit().unwrap();
            }
            self.changed_term_voted_check().await?;
            Ok(())
        }

        /// get current (term, voted_for)
        async fn get_term_voted(&self) -> Res<(u64, Option<NID>)> {
            self.inner.get_term_voted()
        }

        async fn get_max_log_index(&self) -> Res<u64> {
            self.inner.get_max_log_index()
        }

        async fn get_min_log_index(&self) -> Res<u64> {
            self.inner.get_min_log_index()
        }


        /// the payload's meaning depends on the application
        async fn compact_log(&self, snapshot: SnapshotIndexTerm, entries: Vec<LogEntry<T>>) -> Res<()> {
            let opt_value: Option<Vec<IndexValue<T>>> = self.inner.query_key::<Vec<IndexValue<T>>>("value".to_string())?;
            let mut values = HashSet::new();
            let max_index = match entries.last() {
                Some(e) => e.index,
                None => 0,
            };
            for e in entries {
                let _ = values.insert(IndexValue { value: e.value, index: e.index });
            }
            match opt_value {
                Some(vec) => {
                    for v in vec {
                        values.insert(v);
                    }
                }
                None => {}
            }
            self.write_snapshot_gut(snapshot, values.into_iter().collect(),
                                    Some(max_index), None, None).await?;
            self.snapshot_log_changed_check().await?;
            Ok(())
        }

        /// the log would be truncated by index, > index entries would be left; and then the new add entries
        /// would be appended
        async fn write_log_entries(&self, prev_index: u64, log_entries: Vec<LogEntry<T>>, opt: WriteEntriesOpt) -> Res<()> {
            self.inner.write_log_entries(prev_index, log_entries, opt)?;
            self.snapshot_log_changed_check().await?;
            Ok(())
        }

        /// retrieve the log entries in range [start, end)
        async fn read_log_entries(&self, start: u64, end: u64) -> Res<Vec<LogEntry<T>>> {
            self.inner.read_log_entries(start, end)
        }

        /// simple store does not implement iterator
        /// write_snapshot would be atomic in a single invoke
        async fn write_snapshot(
            &self, _id: String,
            snapshot: SnapshotIndexTerm,
            values: Vec<IndexValue<T>>,
            _opt_iter: Option<Vec<u8>>,
            write_opt: WriteSnapshotOpt
        ) -> Res<Option<Vec<u8>>> {
            let truncate_left = if write_opt.truncate_left {
                Some(snapshot.index)
            } else {
                None
            };

            let truncate_right = if write_opt.truncate_right {
                Some(snapshot.index)
            } else {
                None
            };
            self.write_snapshot_gut(snapshot, values,
                                    truncate_left, truncate_right, _opt_iter).await?;
            self.snapshot_log_changed_check().await?;
            Ok(None)
        }

        async fn read_snapshot(&self, _id: String, _opt_iter: Option<Vec<u8>>) -> Res<(SnapshotIndexTerm, Vec<IndexValue<T>>, Option<Vec<u8>>)> {
            let opt: Option<SnapshotIndexTerm> = self.inner.query_key("snapshot".to_string())?;
            let s = if let Some(s) = opt {
                s
            } else {
                SnapshotIndexTerm::default()
            };
            let opt: Option<Vec<IndexValue<T>>> = self.inner.query_key("value".to_string())?;
            let values = if let Some(vec) = opt {
                vec
            } else {
                vec![]
            };
            Ok((s, values, None))
        }

        async fn set_commit_index(&self, index: u64) -> Res<()> {

            {
                let mut conn = self.inner.connection()?;
                let trans = res_sqlite(conn.transaction())?;
                self.inner.set_commit_index(&trans, index)?;
                trans.commit().unwrap();
            }

            self.changed_commit_index_check().await?;
            Ok(())
        }

        async fn setup_store_state(&self, commit_index:Option<u64>, term_voted: Option<(u64, Option<NID>)>, log: Option<Vec<LogEntry<T>>>, snapshot: Option<(SnapshotIndexTerm, Vec<IndexValue<T>>)>) -> Res<()> {
            self.inner.setup_sotre_state(commit_index, term_voted, log, snapshot).await?;
            self.reset_all().await?;
            Ok(())
        }
    }

    impl<T: MsgTrait + 'static> StoreInner<T> {
        fn new(path: String, cluster_name:String) -> Res<Self> {
            let s = Self {
                p: Default::default(),
                path,
                cluster_name,
            };
            s.open()?;
            Ok(s)
        }


        fn write_config(&self, conf: RaftConf) -> Res<()> {
            self.put_key("conf".to_string(), conf)?;
            Ok(())
        }

        ///
        fn read_config(&self) -> Res<RaftConf> {
            let conf = res_option(self.query_key("conf".to_string())?)?;
            Ok(conf)
        }

        fn set_commit_index(&self, trans:&Transaction, commit_index: u64) -> Res<()> {
            self.trans_put_key(trans, "commit_index".to_string(), commit_index)?;
            Ok(())
        }

        /// update term and voted_for
        fn set_term_voted(&self, trans:&Transaction, term: u64, voted_for: Option<NID>) -> Res<()> {
            let t = TermVotedFor {
                term,
                voted_for,
            };
            self.trans_put_key(trans,"term_voted_for".to_string(), t)?;
            Ok(())
        }

        fn get_commit_index(&self) -> Res<u64> {
            let opt: Option<u64> = self.query_key("commit_index".to_string())?;
            if let Some(i) = opt {
                Ok(i)
            } else {
                Ok(0)
            }
        }

        /// get current (term, voted_for)
        fn get_term_voted(&self) -> Res<(u64, Option<NID>)> {
            let opt: Option<TermVotedFor> = self.query_key("term_voted_for".to_string())?;
            if let Some(t) = opt {
                Ok((t.term, t.voted_for))
            } else {
                Ok((0, None))
            }
        }

        fn get_max_log_index(&self) -> Res<u64> {
            self.max_log_index()
        }


        fn get_min_log_index(&self) -> Res<u64> {
            self.min_log_index()
        }

        /// the log would be truncated by index, > index entries would be left; and then the new add entries
        /// would be appended
        fn write_log_entries(&self, prev_index: u64, log_entries: Vec<LogEntry<T>>, opt: WriteEntriesOpt) -> Res<()> {
            let mut conn = self.connection().unwrap();
            let trans = conn.transaction().unwrap();
            self.write_log_gut(&trans, prev_index, log_entries, opt)?;
            trans.commit().unwrap();
            Ok(())
        }

        /// retrieve the log entries in range [start, end)
        fn read_log_entries(&self, start: u64, end: u64) -> Res<Vec<LogEntry<T>>> {
            self.read_log(start, end)
        }

        fn query_key<M: DeserializeOwned + Serialize + 'static>(&self, key: String) -> Res<Option<M>> {
            let c = res_sqlite(Connection::open(self.path.clone()))?;
            let mut s = res_sqlite(c.prepare("select value from meta where key = ?;"))?;
            let mut cursor = s.query([key]).unwrap();

            if let Some(row) = res_sqlite(cursor.next())? {
                let r = row.get(0);
                let s:String = match r {
                    Ok(s) => { s }
                    Err(e) => {
                        error!("error get index 0, {:?}", e);
                        panic!("error");
                    }
                };
                let m: M = serde_json::from_str(s.as_str()).unwrap();
                Ok(Some(m))
            } else {
                Ok(None)
            }
        }

        fn connection(&self) -> Res<Connection> {
            let c = res_sqlite(Connection::open(self.path.clone()))?;
            Ok(c)
        }

        fn put_key<M: DeserializeOwned + Serialize  + 'static>(&self, key: String, value: M) -> Res<()> {
            let mut conn = self.connection()?;
            let trans = res_sqlite(conn.transaction())?;
            self.trans_put_key(&trans, key, value)?;
            res_sqlite(trans.commit())?;
            Ok(())
        }


        fn trans_put_key<M: DeserializeOwned + Serialize  + 'static>(&self, tran: &Transaction, key: String, value: M) -> Res<()> {
            let json = serde_json::to_string(&value).unwrap();
            let _ = tran.execute(r#"insert into meta(key, value) values(?1, ?2)
        on conflict(key) do update set value=?3;"#,
                                 [key.clone(), json.clone(), json.clone()]).unwrap();
            Ok(())
        }

        fn min_log_index(&self) -> Res<u64> {
            let sql = "select entry_index \
                from log \
                order by entry_index asc limit 0, 1;".to_string();
            self.query_rows_index(sql)
        }


        fn max_log_index(&self) -> Res<u64> {
            let sql =
                "select entry_index \
                from log \
                order by entry_index desc limit 0, 1;".to_string();
            self.query_rows_index(sql)
        }

        fn query_rows_index(&self, sql: String) -> Res<u64> {
            let mut c = Connection::open(self.path.clone()).unwrap();
            let t = c.transaction().unwrap();
            let mut stmt = res_sqlite(t.prepare(sql.as_str()))?;
            let mut cursor = res_sqlite(stmt.query([]))?;
            let opt_row = res_sqlite(cursor.next())?;
            match opt_row {
                None => { Ok(0) }
                Some(row) => {
                    let index: u64 = row.get(0).unwrap();
                    Ok(index)
                }
            }
        }
        pub fn write_snapshot_gut(
            &self,
            trans: &Transaction<'_>,
            snapshot: SnapshotIndexTerm,
            values: Vec<IndexValue<T>>,
            truncate_left: Option<u64>,
            truncate_right: Option<u64>,
            _option: Option<Vec<u8>>
        ) -> Res<()> {
            if let Some(_i) = truncate_left {
                trans_truncate_left(&trans, _i)?;
            }
            if let Some(_i) = truncate_right {
                trans_truncate_right_open(&trans, _i)?;
            }
            self.trans_put_key(trans, "value".to_string(), values)?;
            self.trans_put_key(trans, "snapshot".to_string(), snapshot)?;
            Ok(())
        }

        fn write_log_gut(&self, trans: &Transaction, prev_index: u64, log_entries: Vec<LogEntry<T>>, opt: WriteEntriesOpt) -> Res<()> {
            if opt.truncate_left {
                trans_truncate_left(trans, prev_index)?;
            }
            if opt.truncate_right {
                trans_truncate_right_open(trans, prev_index + (log_entries.len() as u64))?;
            }
            let mut _prev_index = prev_index;
            for l in log_entries {
                if _prev_index + 1 != l.index {
                    return Err(ET::FatalError("error log entry index".to_string()));
                }
                _prev_index += 1;
                let json = serde_json::to_string(&l).unwrap();
                let _ = res_sqlite(trans.execute(
                    r#"insert into log(
                        entry_index,
                        entry_term,
                        entry_payload) values(?1, ?2, ?3)
                on conflict(entry_index)
                    do update
                    set
                        entry_term=?4,
                        entry_payload=?5;
                        "#,
                    params![
                        l.index,
                        l.term,
                        json.clone(),
                        l.term,
                        json.clone()
                ],
                ))?;
            }
            Ok(())
        }

        fn read_log(&self, start: u64, end: u64) -> Res<Vec<LogEntry<T>>> {
            let mut c = Connection::open(self.path.clone()).unwrap();
            let mut vec = vec![];
            let t = c.transaction().unwrap();
            {
                let mut s = t.prepare(
                    r#"select entry_index, entry_term, entry_payload
                    from log where entry_index >= ? and entry_index < ?"#).unwrap();
                let mut rows = s.query(params![start, end]).unwrap();

                while let Ok(row) = rows.next() {
                    if let Some(r) = row {
                        let s = r.get::<_, String>(2).unwrap();
                        let e: LogEntry<T> = serde_json::from_str(s.as_str()).unwrap();
                        let entry = LogEntry {
                            term: r.get::<_, u64>(1).unwrap(),
                            index: r.get::<_, u64>(0).unwrap(),
                            value: e.value,
                        };
                        vec.push(entry);
                    } else {
                        break;
                    }
                }
            }
            t.commit().unwrap();
            Ok(vec)
        }

        async fn setup_sotre_state(&self, commit_index:Option<u64>, term_voted: Option<(u64, Option<NID>)>, log: Option<Vec<LogEntry<T>>>, snapshot: Option<(SnapshotIndexTerm, Vec<IndexValue<T>>)>) -> Res<()> {
            let mut c = Connection::open(self.path.clone()).unwrap();
            let trans = c.transaction().unwrap();
            if let Some(_commit_index) = commit_index {
                self.set_commit_index(&trans, _commit_index)?;
            }
            if let Some((_term, _voted)) = term_voted {
                self.set_term_voted(&trans, _term, _voted)?;
            }
            if let Some(_log) = log {
                let prev_index = if let Some(_e) = _log.first() {
                    _e.index - 1
                } else {
                    // when the log is empty, truncate all log entries which :
                    //      index <= 0 && index > 0
                    0
                };
                self.write_log_gut(&trans, prev_index, _log, WriteEntriesOpt {
                    truncate_left: true,
                    truncate_right: true,
                })?;
            }
            if let Some((_snapshot, _values)) = snapshot {
                self.write_snapshot_gut(&trans, _snapshot, _values,
                                        None, None, None)?;
            }
            trans.commit().unwrap();
            Ok(())
        }
        fn open(&self) -> Res<()> {
            let mut c = Connection::open(self.path.clone()).unwrap();
            let t = c.transaction().unwrap();
            res_sqlite(t.execute(
                r#"create table if not exists log (
                    entry_index integer primary key,
                    entry_term integer not null,
                    entry_payload text not null
                ) strict;"#, []))?;
            res_sqlite(t.execute(
                r#"create table if not exists meta (
                    key text primary key,
                    value text not null
                ); strict"#, []))?;
            res_sqlite(t.commit())?;
            Ok(())
        }
    }


    fn trans_truncate_left(trans: &Transaction, index: u64) -> Res<()> {
        res_sqlite(trans.execute(
            r#"delete from log
                where entry_index <= ?1;"#,
            params![index],
        ))?;
        Ok(())
    }

    fn trans_truncate_right_open(trans: &Transaction, index: u64) -> Res<()> {
        res_sqlite(trans.execute(
            r#"delete from log
                where entry_index > ?1;"#,
            params![index],
        ))?;
        Ok(())
    }

    #[derive(
    Serialize,
    Deserialize,
    )]
    pub struct TermVotedFor {
        pub term: u64,
        pub voted_for: Option<NID>,
    }

}

