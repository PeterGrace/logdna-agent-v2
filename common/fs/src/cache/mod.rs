use crate::cache::entry::Entry;
use crate::cache::event::Event;
use crate::cache::tailed_file::TailedFile;
use crate::rule::{GlobRule, Rules, Status};
use notify_stream::{Event as WatchEvent, RecursiveMode, Watcher};

use std::cell::RefCell;
use std::ffi::OsString;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::{fmt, fs, io};

use futures::{Stream, StreamExt};
use slotmap::{DefaultKey, SlotMap};
use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::HashMap;
use thiserror::Error;

pub mod dir_path;
pub mod entry;
pub mod event;
pub mod tailed_file;
pub use dir_path::{DirPathBuf, DirPathBufError};
use metrics::Metrics;
use std::time::Duration;

type WatchDescriptor = PathBuf;
type Children = HashMap<OsString, EntryKey>;
type Symlinks = HashMap<PathBuf, Vec<EntryKey>>;
type WatchDescriptors = HashMap<WatchDescriptor, Vec<EntryKey>>;

pub type EntryKey = DefaultKey;

type EntryMap = SlotMap<EntryKey, entry::Entry>;
type FsResult<T> = Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("error watching: {0:?} {1:?}")]
    Watch(PathBuf, notify_stream::Error),
    #[error("got event for untracked watch descriptor: {0:?}")]
    WatchEvent(WatchDescriptor),
    #[error("the inotify event queue has overflowed and events have presumably been lost")]
    WatchOverflow,
    #[error("unexpected existing entry")]
    Existing,
    #[error("failed to find entry")]
    Lookup,
    #[error("failed to find parent entry")]
    ParentLookup,
    #[error("parent should be a directory")]
    ParentNotValid,
    #[error("path is not valid")]
    PathNotValid(PathBuf),
    #[error("The process lacks permissions to view directory contents")]
    DirectoryListNotValid(io::Error, PathBuf),
    #[error("encountered errors when inserting recursively: {0:?}")]
    InsertRecursively(Vec<Error>),
    #[error("error reading file: {0:?}")]
    File(io::Error),
}

pub struct FileSystem {
    watcher: Watcher,
    pub entries: Rc<RefCell<EntryMap>>,
    root: EntryKey,

    symlinks: Symlinks,
    watch_descriptors: WatchDescriptors,

    master_rules: Rules,
    initial_dirs: Vec<DirPathBuf>,
    initial_dir_rules: Rules,

    initial_events: Vec<Event>,
}

impl FileSystem {
    pub fn new(initial_dirs: Vec<DirPathBuf>, rules: Rules, delay: Duration) -> Self {
        initial_dirs.iter().for_each(|path| {
            if !path.is_dir() {
                panic!("initial dirs must be dirs")
            }
        });

        let watcher = Watcher::new(delay);
        let entries = SlotMap::new();

        let mut initial_dir_rules = Rules::new();
        for path in initial_dirs.iter() {
            append_rules(&mut initial_dir_rules, path.as_ref().into());
        }

        let mut fs = Self {
            entries: Rc::new(RefCell::new(entries)),
            //TODO: Remove field
            root: EntryKey::default(),
            symlinks: Symlinks::new(),
            watch_descriptors: WatchDescriptors::new(),
            master_rules: rules,
            initial_dirs: initial_dirs.clone(),
            initial_dir_rules,
            watcher,
            initial_events: Vec::new(),
        };

        let entries = fs.entries.clone();
        let mut entries = entries.borrow_mut();

        let mut initial_dirs_events = Vec::new();
        for dir in initial_dirs
            .into_iter()
            .map(|path| -> PathBuf { path.into() })
        {
            let mut path_cpy: PathBuf = dir.clone();
            loop {
                if !path_cpy.exists() {
                    path_cpy.pop();
                } else {
                    break;
                }
            }
            if let Err(e) = fs.insert(&path_cpy, &mut initial_dirs_events, &mut entries) {
                // It can failed due to permissions or some other restriction
                debug!(
                    "Initial insertion of {} failed: {}",
                    path_cpy.to_str().unwrap(),
                    e
                );
            }
        }

        for event in initial_dirs_events {
            match event {
                Event::New(entry_key) => {
                    fs.initial_events.push(Event::Initialize(entry_key));
                }
                _ => panic!("unexpected event in initialization"),
            };
        }

        fs
    }

    pub fn stream_events<'a>(fs: Arc<Mutex<FileSystem>>) -> impl Stream<Item = Event> + 'a {
        let events_stream = {
            let watcher = &fs
                .try_lock()
                .expect("could not lock filesystem cache")
                .watcher;
            watcher.receive()
        };

        let initial_events = {
            let mut fs = fs.try_lock().expect("could not lock filesystem cache");

            let mut acc = Vec::new();
            if !fs.initial_events.is_empty() {
                for event in std::mem::replace(&mut fs.initial_events, Vec::new()) {
                    acc.push(event)
                }
            }
            acc
        };

        let events = events_stream.map(move |event| {
            let fs = fs.clone();
            {
                let mut acc = Vec::new();

                fs.try_lock()
                    .expect("couldn't lock filesystem cache")
                    .process(event, &mut acc);
                futures::stream::iter(acc)
            }
        });

        futures::stream::iter(initial_events).chain(events.flatten())
    }

    /// Handles inotify events and may produce Event(s) that are returned upstream through sender
    fn process(&mut self, watch_event: WatchEvent, events: &mut Vec<Event>) {
        let _entries = self.entries.clone();
        let mut _entries = _entries.borrow_mut();

        debug!("handling notify event {:#?}", watch_event);

        // TODO: Remove OsString names
        let result = match watch_event {
            WatchEvent::Create(wd) => self.process_create(&wd, events, &mut _entries),
            //TODO: Handle Write event for directories
            WatchEvent::Write(wd) => self.process_modify(&wd, events),
            WatchEvent::Remove(wd) => self.process_delete(&wd, events, &mut _entries),
            WatchEvent::Rename(from_wd, to_wd) => {
                // Source path should exist and be tracked to be a move
                let is_from_path_ok = self
                    .get_first_entry(&from_wd)
                    .map(|entry| self.entry_path_passes(entry, &_entries))
                    .unwrap_or(false);

                // Target path pass the inclusion/exclusion rules to be a move
                let is_to_path_ok = self.passes(&to_wd, &_entries);

                if is_to_path_ok && is_from_path_ok {
                    self.process_rename(&from_wd, &to_wd, events, &mut _entries)
                } else if is_to_path_ok {
                    self.process_create(&to_wd, events, &mut _entries)
                } else if is_from_path_ok {
                    self.process_delete(&from_wd, events, &mut _entries)
                } else {
                    // Most likely parent was removed, dropping all child watch descriptors
                    // and we've got the child watch event already queued up
                    debug!("Move event received from targets that are not watched anymore");
                    Ok(())
                }
            }
            WatchEvent::Error(e, p) => {
                warn!(
                    "There was an error mapping a file change: {:?} ({:?})",
                    e, p
                );
                Ok(())
            }
            _ => {
                // TODO: Map the rest of the events explicitly
                Ok(())
            }
        };

        if let Err(e) = result {
            match e {
                Error::WatchOverflow => {
                    error!("{}", e);
                    panic!("overflowed kernel queue");
                }
                Error::PathNotValid(path) => {
                    debug!("Path is not longer valid: {:?}", path);
                }
                _ => {
                    warn!("Processing inotify event resulted in error: {}", e);
                }
            }
        }
    }

    fn process_create(
        &mut self,
        watch_descriptor: &WatchDescriptor,
        events: &mut Vec<Event>,
        _entries: &mut EntryMap,
    ) -> FsResult<()> {
        let path = &watch_descriptor;

        //TODO: Check duplicates
        self.insert(&path, events, _entries).map(|_| ())
    }

    fn process_modify(
        &mut self,
        watch_descriptor: &WatchDescriptor,
        events: &mut Vec<Event>,
    ) -> FsResult<()> {
        let mut entry_ptrs_opt = None;
        if let Some(entries) = self.watch_descriptors.get_mut(watch_descriptor) {
            entry_ptrs_opt = Some(entries.clone())
        }

        // TODO: If symlink => revisit target
        if let Some(mut entry_ptrs) = entry_ptrs_opt {
            for entry_ptr in entry_ptrs.iter_mut() {
                events.push(Event::Write(*entry_ptr));
            }
            Ok(())
        } else {
            Err(Error::WatchEvent(watch_descriptor.to_owned()))
        }
    }

    fn process_delete(
        &mut self,
        watch_descriptor: &WatchDescriptor,
        events: &mut Vec<Event>,
        _entries: &mut EntryMap,
    ) -> FsResult<()> {
        let entry_key = self.get_first_entry(watch_descriptor)?;
        let entry = _entries.get(entry_key).ok_or(Error::Lookup)?;
        let path = entry.path().to_path_buf();
        if !self.initial_dirs.iter().any(|dir| dir.as_ref() == path) {
            self.remove(&path, events, _entries)
        } else {
            Ok(())
        }
    }

    /// Inserts a new entry when the path validates the inclusion/exclusion rules.
    ///
    /// Returns `Ok(Some(entry))` pointing to the newly created entry.
    ///
    /// When the path doesn't pass the rules or the path is invalid, it returns `Ok(None)`.
    /// When the file watcher can't be added or the parent dir can not be created, it
    /// returns an `Err`.
    fn insert(
        &mut self,
        path: &Path,
        events: &mut Vec<Event>,
        _entries: &mut EntryMap,
    ) -> FsResult<Option<EntryKey>> {
        if !self.passes(path, _entries) {
            info!("ignoring {:?}", path);
            return Ok(None);
        }

        let link_path = path.read_link();
        if !path.exists() && !link_path.is_ok() {
            warn!("attempted to insert non existent path {:?}", path);
            return Ok(None);
        }

        if fs::metadata(path)
            .map_err(|_| Error::PathNotValid(path.into()))?
            .is_dir()
        {
            // Watch recursively
            let contents =
                fs::read_dir(path).map_err(|e| Error::DirectoryListNotValid(e, path.into()))?;
            // Insert the parent directory first
            trace!("inserting directory {}", path.display());
            let new_entry = Entry::Dir {
                name: path
                    .file_name()
                    .ok_or_else(|| Error::PathNotValid(path.into()))?
                    .to_owned(),
                parent: None,
                children: Default::default(),
                wd: path.into(),
            };

            self.watcher
                .watch(&path, RecursiveMode::NonRecursive)
                .map_err(|e| Error::Watch(path.to_path_buf(), e))?;
            let new_key = self.register_as_child(new_entry, _entries)?;
            events.push(Event::New(new_key));

            for dir_entry in contents {
                if dir_entry.is_err() {
                    continue;
                }
                let dir_entry = dir_entry.unwrap();
                if let Err(e) = self.insert(&dir_entry.path(), events, _entries) {
                    info!(
                        "Error found when inserting child entry for {:?}: {:?}",
                        path, e
                    );
                }
            }
            return Ok(Some(new_key));
        }

        let new_entry = match link_path {
            Ok(target) => {
                trace!(
                    "inserting symlink {} with target {}",
                    path.display(),
                    target.display()
                );
                Entry::Symlink {
                    name: path
                        .file_name()
                        .ok_or_else(|| Error::PathNotValid(path.into()))?
                        .to_owned(),
                    parent: EntryKey::default(),
                    link: target,
                    wd: path.into(),
                    rules: Default::default(),
                }
            }
            _ => {
                trace!("inserting file {}", path.display());
                Metrics::fs().increment_tracked_files();
                Entry::File {
                    name: path
                        .file_name()
                        .ok_or_else(|| Error::PathNotValid(path.into()))?
                        .to_owned(),
                    parent: EntryKey::default(),
                    wd: path.into(),
                    data: RefCell::new(TailedFile::new(path).map_err(Error::File)?),
                }
            }
        };

        self.watcher
            .watch(&path, RecursiveMode::NonRecursive)
            .map_err(|e| Error::Watch(path.to_path_buf(), e))?;
        // TODO: Maybe change method abstractions
        let new_key = self.register_as_child(new_entry, _entries)?;
        events.push(Event::New(new_key));
        Ok(Some(new_key))
    }

    fn register(&mut self, entry_key: EntryKey, _entries: &mut EntryMap) -> FsResult<()> {
        let entry = _entries.get(entry_key).ok_or(Error::Lookup)?;
        let path = entry.path();

        self.watch_descriptors
            .entry(path.to_path_buf())
            .or_insert_with(Vec::new)
            .push(entry_key);

        if let Entry::Symlink { link, .. } = entry.deref() {
            self.symlinks
                .entry(link.clone())
                .or_insert_with(Vec::new)
                .push(entry_key);
        }

        info!("watching {:?}", path);
        Ok(())
    }

    /// Removes the entry reference from watch_descriptors and symlinks
    fn unregister(&mut self, entry_key: EntryKey, _entries: &mut EntryMap) {
        let entry = match _entries.get(entry_key) {
            Some(v) => v,
            None => {
                error!("failed to find entry to unregister");
                return;
            }
        };

        let path = entry.path().to_path_buf();
        let entries = match self.watch_descriptors.get_mut(&path) {
            Some(v) => v,
            None => {
                error!("attempted to remove untracked watch descriptor {:?}", path);
                return;
            }
        };

        entries.retain(|other| *other != entry_key);
        if entries.is_empty() {
            self.watch_descriptors.remove(&path);
            if let Err(e) = self.watcher.unwatch_if_exists(&path) {
                // Log and continue
                debug!(
                    "unwatching {:?} resulted in an error, likely due to a dangling symlink {:?}",
                    path, e
                );
            }
        }

        if let Entry::Symlink { link, .. } = entry.deref() {
            let entries = match self.symlinks.get_mut(link) {
                Some(v) => v,
                None => {
                    error!("attempted to remove untracked symlink {:?}", path);
                    return;
                }
            };

            entries.retain(|other| *other != entry_key);
            if entries.is_empty() {
                self.symlinks.remove(link);
            }
        }

        info!("unwatching {:?}", path);
    }

    fn remove(
        &mut self,
        path: &Path,
        events: &mut Vec<Event>,
        _entries: &mut EntryMap,
    ) -> FsResult<()> {
        let entry_key = self.lookup(path, _entries).ok_or(Error::Lookup)?;
        let parent = path.parent().map(|p| self.lookup(p, _entries)).flatten();

        if let Some(parent) = parent {
            let name = path
                .file_name()
                .ok_or_else(|| Error::PathNotValid(path.to_path_buf()))?;
            match _entries.get_mut(parent) {
                None => {}
                Some(parent_entry) => {
                    parent_entry
                        .children_mut()
                        .ok_or(Error::ParentNotValid)?
                        .remove(&name.to_owned());
                }
            }
        }

        self.drop_entry(entry_key, events, _entries);

        Ok(())
    }

    /// Emits `Delete` events, removes the entry and its children from
    /// watch descriptors and symlinks.
    fn drop_entry(
        &mut self,
        entry_key: EntryKey,
        events: &mut Vec<Event>,
        _entries: &mut EntryMap,
    ) {
        self.unregister(entry_key, _entries);
        if let Some(entry) = _entries.get(entry_key) {
            let mut _children = vec![];
            let mut _links = vec![];
            match entry.deref() {
                Entry::Dir { children, .. } => {
                    for child in children.values() {
                        _children.push(*child);
                    }
                }
                Entry::Symlink { ref link, .. } => {
                    // This is a hacky way to check if there are any remaining
                    // symlinks pointing to `link`
                    if !self.passes(link, _entries) {
                        _links.push(link.clone())
                    }

                    events.push(Event::Delete(entry_key));
                }
                Entry::File { .. } => {
                    Metrics::fs().decrement_tracked_files();
                    events.push(Event::Delete(entry_key));
                }
            }

            for child in _children {
                self.drop_entry(child, events, _entries);
            }

            for link in _links {
                // Ignore error
                self.remove(&link, events, _entries).unwrap_or_default();
            }
        }
    }

    // `from` is the path from where the file or dir used to live
    // `to is the path to where the file or dir now lives
    // e.g from = /var/log/syslog and to = /var/log/syslog.1.log
    fn process_rename(
        &mut self,
        from_path: &Path,
        to_path: &Path,
        events: &mut Vec<Event>,
        _entries: &mut EntryMap,
    ) -> FsResult<()> {
        let new_parent = to_path.parent().map(|p| self.lookup(p, _entries)).flatten();

        match self.lookup(from_path, _entries) {
            Some(entry_key) => {
                let entry = _entries.get_mut(entry_key).ok_or(Error::Lookup)?;
                let new_name = to_path
                    .file_name()
                    .ok_or_else(|| Error::PathNotValid(to_path.into()))?
                    .to_owned();
                let old_name = entry.name().clone();
                //TODO: Remove parent() and navigate using paths
                if let Some(parent) = entry.parent() {
                    _entries
                        .get_mut(parent)
                        .ok_or(Error::ParentLookup)?
                        .children_mut()
                        .ok_or(Error::ParentNotValid)?
                        .remove(&old_name);
                }

                let entry = _entries.get_mut(entry_key).ok_or(Error::Lookup)?;
                entry.set_name(new_name.clone());
                entry.set_path(to_path.to_path_buf());

                // Remove previous reference and add new one
                self.watch_descriptors.remove(to_path);
                self.watch_descriptors
                    .entry(to_path.to_path_buf())
                    .or_insert_with(Vec::new)
                    .push(entry_key);

                if let Some(new_parent) = new_parent {
                    entry.set_parent(new_parent);

                    _entries
                        .get_mut(new_parent)
                        .ok_or(Error::ParentLookup)?
                        .children_mut()
                        .ok_or(Error::ParentNotValid)?
                        .insert(new_name, entry_key);
                }
            }
            None => {
                self.insert(to_path, events, _entries)?;
            }
        }
        Ok(())
    }

    /// Inserts the entry, registers it, looks up for the parent and set itself as a children.
    fn register_as_child(
        &mut self,
        new_entry: Entry,
        entries: &mut EntryMap,
    ) -> FsResult<EntryKey> {
        let component = new_entry.name().clone();
        let parent_path = new_entry.path().parent().map(|p| p.to_path_buf());
        let new_key = entries.insert(new_entry);
        self.register(new_key, entries)?;

        // Try to find parent
        if let Some(parent_path) = parent_path {
            if let Some(parent_key) = self.watch_descriptors.get(&parent_path) {
                let parent_key = parent_key.get(0).copied().ok_or(Error::ParentLookup)?;
                return match entries
                    .get_mut(parent_key)
                    .ok_or(Error::ParentLookup)?
                    .children_mut()
                    .ok_or(Error::ParentNotValid)?
                    .entry(component)
                {
                    HashMapEntry::Vacant(v) => Ok(*v.insert(new_key)),
                    // TODO: Maybe consider to silently replace
                    _ => Err(Error::Existing),
                };
            } else {
                trace!("Parent with path {:?} not found", parent_path);
            }
        }

        // Parent was not found but it's actively tracked
        Ok(new_key)
    }

    /// Returns the entry that represents the supplied path.
    /// When the path is not represented and therefore has no entry then `None` is return.
    pub fn lookup(&self, path: &Path, _entries: &EntryMap) -> Option<EntryKey> {
        self.watch_descriptors.get(path).map(|entries| entries[0])
    }

    fn is_symlink_target(&self, path: &Path, _entries: &EntryMap) -> bool {
        for (_, symlink_ptrs) in self.symlinks.iter() {
            for symlink_ptr in symlink_ptrs.iter() {
                if let Some(symlink) = _entries.get(*symlink_ptr) {
                    match symlink {
                        Entry::Symlink { rules, .. } => {
                            if let Status::Ok = rules.passes(path) {
                                if let Status::Ok = self.master_rules.included(path) {
                                    return true;
                                }
                            }
                        }
                        _ => {
                            panic!(
                                "did not expect non symlink entry in symlinks master map for path {:?}",
                                path
                            );
                        }
                    }
                } else {
                    error!("failed to find entry");
                };
            }
        }
        false
    }

    /// Determines whether the path is within the initial dir
    /// and either passes the master rules (e.g. "*.log") or it's a directory
    pub(crate) fn is_initial_dir_target(&self, path: &Path) -> bool {
        // Must be within the initial dir
        if self.initial_dir_rules.passes(path) != Status::Ok {
            return false;
        }

        // The file should validate the file rules or be a directory
        if self.master_rules.passes(path) != Status::Ok {
            if let Ok(metadata) = std::fs::metadata(path) {
                return metadata.is_dir();
            }
            return false;
        }

        true
    }

    /// Helper method for checking if a path passes exclusion/inclusion rules
    fn passes(&self, path: &Path, _entries: &EntryMap) -> bool {
        self.is_initial_dir_target(path) || self.is_symlink_target(path, _entries)
    }

    fn entry_path_passes(&self, entry_key: EntryKey, entries: &EntryMap) -> bool {
        entries
            .get(entry_key)
            .map(|e| self.passes(e.path(), &entries))
            .unwrap_or(false)
    }

    /// Returns the first entry based on the `WatchDescriptor`, returning an `Err` when not found.
    fn get_first_entry(&self, wd: &WatchDescriptor) -> FsResult<EntryKey> {
        let entries = self
            .watch_descriptors
            .get(wd)
            .ok_or_else(|| Error::WatchEvent(wd.to_owned()))?;

        if !entries.is_empty() {
            Ok(entries[0])
        } else {
            Err(Error::WatchEvent(wd.to_owned()))
        }
    }
}

// conditionally implement std::fmt::Debug if the underlying type T implements it
impl fmt::Debug for FileSystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("FileSystem");
        builder.field("root", &&self.root);
        builder.field("symlinks", &&self.symlinks);
        builder.field("watch_descriptors", &&self.watch_descriptors);
        builder.field("master_rules", &&self.master_rules);
        builder.field("initial_dir_rules", &&self.initial_dir_rules);
        builder.field("initial_events", &&self.initial_events);
        builder.finish()
    }
}

// Attach rules for all sub paths for a path
fn append_rules(rules: &mut Rules, mut path: PathBuf) {
    rules.add_inclusion(
        GlobRule::new(path.join(r"**").to_str().expect("invalid unicode in path"))
            .expect("invalid glob rule format"),
    );

    loop {
        rules.add_inclusion(
            GlobRule::new(path.to_str().expect("invalid unicode in path"))
                .expect("invalid glob rule format"),
        );
        if !path.pop() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rule::{GlobRule, Rules};
    use crate::test::LOGGER;
    use pin_utils::pin_mut;
    use std::convert::TryInto;
    use std::fs::{copy, create_dir, hard_link, remove_dir_all, remove_file, rename, File};
    use std::os::unix::fs::symlink;
    use std::{io, panic};
    use tempfile::{tempdir, TempDir};

    static DELAY: Duration = Duration::from_millis(200);

    macro_rules! take_events {
        ( $x:expr, $y: expr ) => {{
            use tokio_stream::StreamExt;

            tokio_test::block_on(async {
                futures::StreamExt::collect::<Vec<_>>(futures::StreamExt::take(
                    FileSystem::stream_events($x.clone())
                        .timeout(std::time::Duration::from_millis(500)),
                    $y,
                ))
                .await
            })
        }};
    }

    macro_rules! take {
        ($x: expr) => {
            tokio::time::sleep(DELAY * 2).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            let stream = FileSystem::stream_events($x.clone());
            pin_mut!(stream);
            loop {
                tokio::select! {
                    _ = stream.next() => {}
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        break;
                    }
                }
            }
        };
    }

    macro_rules! lookup {
        ( $x:expr, $y: expr ) => {{
            let fs = $x.lock().expect("failed to lock fs");
            let entry_keys = fs.watch_descriptors.get(&$y);
            if entry_keys.is_none() {
                None
            } else {
                Some(entry_keys.unwrap()[0])
            }
        }};
    }

    macro_rules! assert_is_file {
        ( $x:expr, $y: expr ) => {
            assert!($y.is_some());
            {
                let fs = $x.lock().expect("couldn't lock fs");
                let entries = fs.entries.borrow();
                assert!(matches!(
                    entries.get($y.unwrap()).unwrap().deref(),
                    Entry::File { .. }
                ));
            }
        };
    }

    macro_rules! lookup_entry {
        ( $x:expr, $y: expr ) => {{
            let fs = $x.lock().expect("failed to lock fs");
            let entries = fs.entries.clone();
            let entries = entries.borrow();
            fs.lookup(&$y, &entries)
        }};
    }

    fn new_fs<T: Default + Clone + std::fmt::Debug>(
        path: PathBuf,
        rules: Option<Rules>,
    ) -> FileSystem {
        let rules = rules.unwrap_or_else(|| {
            let mut rules = Rules::new();
            rules.add_inclusion(GlobRule::new(r"**").unwrap());
            rules
        });
        FileSystem::new(
            vec![path
                .as_path()
                .try_into()
                .unwrap_or_else(|_| panic!("{:?} is not a directory!", path))],
            rules,
            DELAY,
        )
    }

    fn create_fs(path: &Path) -> Arc<Mutex<FileSystem>> {
        Arc::new(Mutex::new(new_fs::<()>(path.to_path_buf(), None)))
    }

    fn run_test<T: FnOnce() + panic::UnwindSafe>(test: T) {
        #![allow(unused_must_use, clippy::clone_on_copy)]
        LOGGER.clone();
        let result = panic::catch_unwind(|| {
            test();
        });

        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn filesystem_init_test() {
        let temp_dir = tempdir().unwrap();
        let dir = temp_dir.path();

        let file_path = dir.join("a.log");
        File::create(&file_path).unwrap();

        let fs = create_fs(dir);

        take!(fs);

        let entry_key = lookup!(fs, file_path);
        assert_is_file!(fs, entry_key);
    }

    // Simulates the `create_move` log rotation strategy
    #[tokio::test]
    async fn filesystem_rotate_create_move() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path();

        let fs = create_fs(path);

        tokio::time::sleep(Duration::from_millis(200)).await;
        let a = path.join("a");
        File::create(&a).unwrap();

        take!(fs);
        let entry_key = lookup!(fs, a);
        assert_is_file!(fs, entry_key);

        let new = path.join("a.new");
        rename(&a, &new).unwrap();

        take!(fs);

        // Previous name should not have an associated entry
        let entry_key = lookup!(fs, a);
        assert!(entry_key.is_none());

        let entry_key = lookup!(fs, new);
        assert_is_file!(fs, entry_key);

        // Create a new file in place
        File::create(&a).unwrap();

        take!(fs);
        let entry_key = lookup!(fs, a);
        assert_is_file!(fs, entry_key);
    }

    // Simulates the `create_copy` log rotation strategy
    #[tokio::test]
    async fn filesystem_rotate_create_copy() -> io::Result<()> {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().to_path_buf();
        let fs = create_fs(&path);

        let a = path.join("a");
        File::create(&a)?;
        take!(fs);
        let entry_key = lookup!(fs, a);
        assert_is_file!(fs, entry_key);

        // Copy and remove
        let old = path.join("a.old");
        copy(&a, &old)?;
        remove_file(&a)?;

        take!(fs);
        let entry_key = lookup!(fs, a);
        assert!(entry_key.is_none());
        let entry_key = lookup!(fs, old);
        assert_is_file!(fs, entry_key);

        // Recreate original file back
        File::create(&a)?;
        take!(fs);
        let entry_key = lookup!(fs, a);
        assert_is_file!(fs, entry_key);

        Ok(())
    }

    // Creates a plain old dir
    #[tokio::test]
    async fn filesystem_create_dir() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().to_path_buf();

        let fs = create_fs(&path);
        take!(fs);
        let entry_key = lookup!(fs, path);
        assert!(entry_key.is_some());

        let fs = fs.lock().expect("couldn't lock fs");
        let entries = fs.entries.borrow();
        assert!(matches!(
            entries.get(entry_key.unwrap()).unwrap().deref(),
            Entry::Dir { .. }
        ));
    }

    /// Creates a dir w/ dots and a file after initialization
    #[tokio::test]
    async fn filesystem_create_dir_after_init() -> io::Result<()> {
        let tempdir = TempDir::new()?;
        let path = tempdir.path().to_path_buf();

        let fs = create_fs(&path);
        take!(fs);

        // Use a subdirectory with dots
        let sub_dir = path.join("sub.dir");
        create_dir(&sub_dir)?;
        let file_path = sub_dir.join("insert.log");
        File::create(&file_path)?;

        take!(fs);
        let entry_key = lookup!(fs, file_path);
        assert_is_file!(fs, entry_key);
        Ok(())
    }

    // Creates a plain old file
    #[tokio::test]
    async fn filesystem_create_file() -> io::Result<()> {
        let tempdir = TempDir::new()?;
        let path = tempdir.path().to_path_buf();

        let fs = create_fs(&path);
        let file_path = path.join("insert.log");
        File::create(&file_path)?;
        take!(fs);

        let entry_key = lookup!(fs, file_path);
        assert_is_file!(fs, entry_key);
        Ok(())
    }

    // Creates a symlink
    #[tokio::test]
    async fn filesystem_create_symlink() -> io::Result<()> {
        let _ = env_logger::Builder::from_default_env().try_init();
        let tempdir = TempDir::new()?;
        let path = tempdir.path().to_path_buf();

        let fs = create_fs(&path);

        let a = path.join("a");
        let b = path.join("b");
        create_dir(&a)?;
        symlink(&a, &b)?;

        take!(fs);

        let entry = lookup!(fs, a);
        assert!(entry.is_some());
        let entry2 = lookup!(fs, b);
        assert!(entry.is_some());
        let _fs = fs.lock().expect("couldn't lock fs");
        let _entries = &_fs.entries;
        let _entries = _entries.borrow();
        match _entries.get(entry.unwrap()).unwrap().deref() {
            Entry::Dir { .. } => {}
            _ => panic!("wrong entry type"),
        };

        match _entries.get(entry2.unwrap()).unwrap().deref() {
            Entry::Symlink { link, .. } => {
                assert_eq!(*link, a);
            }
            Entry::Dir { .. } => {
                panic!("is dir");
            }
            Entry::File { .. } => {
                panic!("is dir");
            }
        };

        Ok(())
    }

    // Creates a hardlink
    #[test]
    fn filesystem_create_hardlink() {
        run_test(|| {
            let tempdir = TempDir::new().unwrap();
            let path = tempdir.path().to_path_buf();

            let fs = Arc::new(Mutex::new(new_fs::<()>(path.clone(), None)));

            let file_path = path.join("insert.log");
            let hard_path = path.join("hard.log");
            File::create(file_path.clone()).unwrap();
            hard_link(&file_path, &hard_path).unwrap();

            take_events!(fs, 2);

            let entry = lookup_entry!(fs, file_path).unwrap();
            let entry2 = lookup_entry!(fs, hard_path);
            let real_watch_descriptor;
            let _fs = fs.lock().expect("couldn't lock fs");
            let _entries = &_fs.entries;
            let _entries = _entries.borrow();
            let _entry = _entries.get(entry).unwrap();
            match _entry.deref() {
                Entry::File { wd, .. } => {
                    real_watch_descriptor = wd;
                }
                _ => panic!("wrong entry type"),
            };

            assert!(entry2.is_some());
            match _entries.get(entry2.unwrap()).unwrap().deref() {
                Entry::File { ref wd, .. } => assert_eq!(wd, real_watch_descriptor),
                _ => panic!("wrong entry type"),
            };
        });
    }

    // Deletes a directory
    #[test]
    fn filesystem_delete_filled_dir() {
        run_test(|| {
            let tempdir = TempDir::new().unwrap();
            let path = tempdir.path().to_path_buf();

            let file_path = path.join("file.log");
            let sym_path = path.join("sym.log");
            let hard_path = path.join("hard.log");
            File::create(file_path.clone()).unwrap();
            symlink(&file_path, &sym_path).unwrap();
            hard_link(&file_path, &hard_path).unwrap();

            let fs = Arc::new(Mutex::new(new_fs::<()>(path.clone(), None)));

            assert!(lookup_entry!(fs, path).is_some());
            assert!(lookup_entry!(fs, file_path).is_some());
            assert!(lookup_entry!(fs, sym_path).is_some());
            assert!(lookup_entry!(fs, hard_path).is_some());

            tempdir.close().unwrap();
            take_events!(fs, 7);

            // It's a root dir, make sure it's still there
            assert!(lookup_entry!(fs, path).is_some());
            assert!(lookup_entry!(fs, file_path).is_none());
            assert!(lookup_entry!(fs, sym_path).is_none());
            assert!(lookup_entry!(fs, hard_path).is_none());
        });
    }

    // Deletes a directory
    #[test]
    fn filesystem_delete_nested_filled_dir() {
        run_test(|| {
            // Now make a nested dir
            let rootdir = TempDir::new().unwrap();
            let rootpath = rootdir.path().to_path_buf();

            let tempdir = TempDir::new_in(rootpath.clone()).unwrap();
            let path = tempdir.path().to_path_buf();

            let file_path = path.join("file.log");
            let sym_path = path.join("sym.log");
            let hard_path = path.join("hard.log");
            File::create(file_path.clone()).unwrap();
            symlink(&file_path, &sym_path).unwrap();
            hard_link(&file_path, &hard_path).unwrap();

            let fs = Arc::new(Mutex::new(new_fs::<()>(rootpath, None)));

            assert!(lookup_entry!(fs, path).is_some());
            assert!(lookup_entry!(fs, file_path).is_some());
            assert!(lookup_entry!(fs, sym_path).is_some());
            assert!(lookup_entry!(fs, hard_path).is_some());

            tempdir.close().unwrap();

            take_events!(fs, 7);
            assert!(lookup_entry!(fs, path).is_none());
            assert!(lookup_entry!(fs, file_path).is_none());
            assert!(lookup_entry!(fs, sym_path).is_none());
            assert!(lookup_entry!(fs, hard_path).is_none());
        });
    }

    // Deletes a file
    #[test]
    fn filesystem_delete_file() {
        run_test(|| {
            let tempdir = TempDir::new().unwrap();
            let path = tempdir.path().to_path_buf();

            let file_path = path.join("file");
            File::create(file_path.clone()).unwrap();

            let fs = Arc::new(Mutex::new(new_fs::<()>(path, None)));

            assert!(lookup_entry!(fs, file_path).is_some());

            remove_file(&file_path).unwrap();
            take_events!(fs, 2);

            assert!(lookup_entry!(fs, file_path).is_none());
        });
    }

    // Deletes a symlink
    #[test]
    fn filesystem_delete_symlink() {
        run_test(|| {
            let tempdir = TempDir::new().unwrap();
            let path = tempdir.path().to_path_buf();

            let a = path.join("a");
            let b = path.join("b");
            create_dir(&a).unwrap();
            symlink(&a, &b).unwrap();

            let fs = Arc::new(Mutex::new(new_fs::<()>(path, None)));

            remove_dir_all(&b).unwrap();
            take_events!(fs, 1);

            assert!(lookup_entry!(fs, a).is_some());
            assert!(lookup_entry!(fs, b).is_none());
        });
    }

    /// Deletes a symlink that points to a not tracked directory
    #[test]
    fn filesystem_delete_symlink_to_untracked_dir() -> io::Result<()> {
        let tempdir = TempDir::new()?;
        let tempdir2 = TempDir::new()?.into_path();
        let path = tempdir.path().to_path_buf();

        let real_dir_path = tempdir2.join("real_dir_sample");
        let symlink_path = path.join("symlink_sample");
        create_dir(&real_dir_path)?;
        symlink(&real_dir_path, &symlink_path)?;

        let fs = Arc::new(Mutex::new(new_fs::<()>(path, None)));
        assert!(lookup_entry!(fs, symlink_path).is_some());
        assert!(lookup_entry!(fs, real_dir_path).is_some());

        remove_dir_all(&symlink_path)?;
        take_events!(fs, 1);

        assert!(lookup_entry!(fs, symlink_path).is_none());
        assert!(lookup_entry!(fs, real_dir_path).is_none());
        Ok(())
    }

    // Deletes the pointee of a symlink
    #[test]
    fn filesystem_delete_symlink_pointee() {
        run_test(|| {
            let tempdir = TempDir::new().unwrap();
            let path = tempdir.path().to_path_buf();

            let a = path.join("a");
            let b = path.join("b");
            create_dir(&a).unwrap();
            symlink(&a, &b).unwrap();

            let fs = Arc::new(Mutex::new(new_fs::<()>(path, None)));

            remove_dir_all(&a).unwrap();
            take_events!(fs, 1);

            assert!(lookup_entry!(fs, a).is_none());
            assert!(lookup_entry!(fs, b).is_some());
        });
    }

    // Deletes a hardlink
    #[test]
    fn filesystem_delete_hardlink() {
        run_test(|| {
            let tempdir = TempDir::new().unwrap();
            let path = tempdir.path().to_path_buf();

            let a = path.join("a");
            let b = path.join("b");
            File::create(a.clone()).unwrap();
            hard_link(&a, &b).unwrap();

            let fs = Arc::new(Mutex::new(new_fs::<()>(path, None)));

            assert!(lookup_entry!(fs, a).is_some());
            assert!(lookup_entry!(fs, b).is_some());

            remove_file(&b).unwrap();
            take_events!(fs, 3);

            assert!(lookup_entry!(fs, a).is_some());
            assert!(lookup_entry!(fs, b).is_none());
        });
    }

    // Deletes the pointee of a hardlink (not totally accurate since we're not deleting the inode
    // entry, but what evs)
    #[test]
    fn filesystem_delete_hardlink_pointee() {
        run_test(|| {
            let tempdir = TempDir::new().unwrap();
            let path = tempdir.path().to_path_buf();

            let a = path.join("a");
            let b = path.join("b");
            File::create(a.clone()).unwrap();
            hard_link(&a, &b).unwrap();

            let fs = Arc::new(Mutex::new(new_fs::<()>(path, None)));

            remove_file(&a).unwrap();
            take_events!(fs, 3);

            assert!(lookup_entry!(fs, a).is_none());
            assert!(lookup_entry!(fs, b).is_some());
        });
    }

    // Moves a directory within the watched directory
    #[test]
    fn filesystem_move_dir_internal() {
        run_test(|| {
            let tempdir = TempDir::new().unwrap();
            let path = tempdir.path().to_path_buf();

            let old_dir_path = path.join("old");
            let new_dir_path = path.join("new");
            let file_path = old_dir_path.join("file.log");
            let sym_path = old_dir_path.join("sym.log");
            let hard_path = old_dir_path.join("hard.log");
            create_dir(&old_dir_path).unwrap();
            File::create(file_path.clone()).unwrap();
            symlink(&file_path, &sym_path).unwrap();
            hard_link(&file_path, &hard_path).unwrap();

            let fs = Arc::new(Mutex::new(new_fs::<()>(path, None)));

            rename(&old_dir_path, &new_dir_path).unwrap();
            take_events!(fs, 4);

            assert!(lookup_entry!(fs, old_dir_path).is_none());
            assert!(lookup_entry!(fs, file_path).is_none());
            assert!(lookup_entry!(fs, sym_path).is_none());
            assert!(lookup_entry!(fs, hard_path).is_none());

            let entry = lookup_entry!(fs, new_dir_path);
            assert!(entry.is_some());

            let entry2 = lookup_entry!(fs, new_dir_path.join("file.log"));
            assert!(entry2.is_some());

            let entry3 = lookup_entry!(fs, new_dir_path.join("hard.log"));
            assert!(entry3.is_some());

            let entry4 = lookup_entry!(fs, new_dir_path.join("sym.log"));
            assert!(entry4.is_some());

            let _fs = fs.lock().expect("couldn't lock fs");
            let _entries = &_fs.entries;
            let _entries = _entries.borrow();
            match _entries.get(entry.unwrap()).unwrap().deref() {
                Entry::Dir { .. } => {}
                _ => panic!("wrong entry type"),
            };

            match _entries.get(entry2.unwrap()).unwrap().deref() {
                Entry::File { .. } => {}
                _ => panic!("wrong entry type"),
            };

            match _entries.get(entry3.unwrap()).unwrap().deref() {
                Entry::File { .. } => {}
                _ => panic!("wrong entry type"),
            };

            match _entries.get(entry4.unwrap()).unwrap().deref() {
                Entry::Symlink { link, .. } => {
                    // symlinks don't update so this link is bad
                    assert_eq!(*link, file_path);
                }
                _ => panic!("wrong entry type"),
            };
        });
    }

    // Moves a directory out
    #[test]
    fn filesystem_move_dir_out() {
        run_test(|| {
            let tempdir = TempDir::new().unwrap();
            let path = tempdir.path().to_path_buf();

            let old_dir_path = path.join("old");
            let new_dir_path = path.join("new");
            let file_path = old_dir_path.join("file.log");
            let sym_path = old_dir_path.join("sym.log");
            let hard_path = old_dir_path.join("hard.log");
            create_dir(&old_dir_path).unwrap();
            File::create(file_path.clone()).unwrap();
            symlink(&file_path, &sym_path).unwrap();
            hard_link(&file_path, &hard_path).unwrap();

            let fs = Arc::new(Mutex::new(new_fs::<()>(old_dir_path.clone(), None)));

            rename(&old_dir_path, &new_dir_path).unwrap();
            take_events!(fs, 1);

            assert!(lookup_entry!(fs, new_dir_path).is_none());
            assert!(lookup_entry!(fs, new_dir_path.join("file.log")).is_none());
            assert!(lookup_entry!(fs, new_dir_path.join("hard.log")).is_none());
            assert!(lookup_entry!(fs, new_dir_path.join("sym.log")).is_none());
        });
    }

    // Moves a directory in
    #[test]
    fn filesystem_move_dir_in() {
        run_test(|| {
            let old_tempdir = TempDir::new().unwrap();
            let old_path = old_tempdir.path().to_path_buf();

            let new_tempdir = TempDir::new().unwrap();
            let new_path = new_tempdir.path().to_path_buf();

            let old_dir_path = old_path.join("old");
            let new_dir_path = new_path.join("new");
            let file_path = old_dir_path.join("file.log");
            let sym_path = old_dir_path.join("sym.log");
            let hard_path = old_dir_path.join("hard.log");
            create_dir(&old_dir_path).unwrap();
            File::create(file_path.clone()).unwrap();
            symlink(&file_path, &sym_path).unwrap();
            hard_link(&file_path, &hard_path).unwrap();

            let fs = Arc::new(Mutex::new(new_fs::<()>(new_path, None)));

            assert!(lookup_entry!(fs, old_dir_path).is_none());
            assert!(lookup_entry!(fs, new_dir_path).is_none());
            assert!(lookup_entry!(fs, file_path).is_none());
            assert!(lookup_entry!(fs, sym_path).is_none());
            assert!(lookup_entry!(fs, hard_path).is_none());

            rename(&old_dir_path, &new_dir_path).unwrap();
            take_events!(fs, 2);

            let entry = lookup_entry!(fs, new_dir_path);
            assert!(entry.is_some());

            let entry2 = lookup_entry!(fs, new_dir_path.join("file.log"));
            assert!(entry2.is_some());

            let entry3 = lookup_entry!(fs, new_dir_path.join("hard.log"));
            assert!(entry3.is_some());

            let entry4 = lookup_entry!(fs, new_dir_path.join("sym.log"));
            assert!(entry4.is_some());

            let _fs = fs.lock().expect("couldn't lock fs");
            let _entries = &_fs.entries;
            let _entries = _entries.borrow();
            match _entries.get(entry.unwrap()).unwrap().deref() {
                Entry::Dir { .. } => {}
                _ => panic!("wrong entry type"),
            };

            match _entries.get(entry2.unwrap()).unwrap().deref() {
                Entry::File { .. } => {}
                _ => panic!("wrong entry type"),
            };

            match _entries.get(entry3.unwrap()).unwrap().deref() {
                Entry::File { .. } => {}
                _ => panic!("wrong entry type"),
            };

            match _entries.get(entry4.unwrap()).unwrap().deref() {
                Entry::Symlink { link, .. } => {
                    // symlinks don't update so this link is bad
                    assert_eq!(*link, file_path);
                }
                _ => panic!("wrong entry type"),
            };
        });
    }

    // Moves a file within the watched directory
    #[test]
    fn filesystem_move_file_internal() {
        run_test(|| {
            let tempdir = TempDir::new().unwrap();
            let path = tempdir.path().to_path_buf();

            let fs = Arc::new(Mutex::new(new_fs::<()>(path.clone(), None)));

            let file_path = path.join("insert.log");
            let new_path = path.join("new.log");
            File::create(file_path.clone()).unwrap();
            rename(&file_path, &new_path).unwrap();

            take_events!(fs, 1);

            let entry = lookup_entry!(fs, file_path);
            assert!(entry.is_none());

            let entry = lookup_entry!(fs, new_path);
            assert!(entry.is_some());
            let _fs = fs.lock().expect("couldn't lock fs");
            let _entries = &_fs.entries;
            let _entries = _entries.borrow();
            match _entries.get(entry.unwrap()).unwrap().deref() {
                Entry::File { .. } => {}
                _ => panic!("wrong entry type"),
            };
        });
    }

    // Moves a file out of the watched directory
    #[test]
    fn filesystem_move_file_out() {
        run_test(|| {
            let tempdir = TempDir::new().unwrap();
            let path = tempdir.path().to_path_buf();

            let watch_path = path.join("watch");
            let other_path = path.join("other");
            create_dir(&watch_path).unwrap();
            create_dir(&other_path).unwrap();

            let file_path = watch_path.join("inside.log");
            let move_path = other_path.join("outside.log");
            File::create(file_path.clone()).unwrap();

            let fs = Arc::new(Mutex::new(new_fs::<()>(watch_path, None)));

            rename(&file_path, &move_path).unwrap();

            take_events!(fs, 2);

            let entry = lookup_entry!(fs, file_path);
            assert!(entry.is_none());

            let entry = lookup_entry!(fs, move_path);
            assert!(entry.is_none());
        });
    }

    // Moves a file into the watched directory
    #[test]
    fn filesystem_move_file_in() {
        run_test(|| {
            let tempdir = TempDir::new().unwrap();
            let path = tempdir.path().to_path_buf();

            let watch_path = path.join("watch");
            let other_path = path.join("other");
            create_dir(&watch_path).unwrap();
            create_dir(&other_path).unwrap();

            let file_path = other_path.join("inside.log");
            let move_path = watch_path.join("outside.log");
            File::create(file_path.clone()).unwrap();

            let fs = Arc::new(Mutex::new(new_fs::<()>(watch_path, None)));

            rename(&file_path, &move_path).unwrap();
            File::create(file_path.clone()).unwrap();

            take_events!(fs, 1);

            let entry = lookup_entry!(fs, file_path);
            assert!(entry.is_none());

            let entry = lookup_entry!(fs, move_path);
            assert!(entry.is_some());
            let _fs = fs.lock().expect("couldn't lock fs");
            let _entries = &_fs.entries;
            let _entries = _entries.borrow();
            match _entries.get(entry.unwrap()).unwrap().deref() {
                Entry::File { .. } => {}
                _ => panic!("wrong entry type"),
            };
        });
    }

    // Moves a file out of the watched directory
    #[test]
    fn filesystem_move_symlink_file_out() {
        run_test(|| {
            let tempdir = TempDir::new().unwrap();
            let path = tempdir.path().to_path_buf();

            let watch_path = path.join("watch");
            let other_path = path.join("other");
            create_dir(&watch_path).unwrap();
            create_dir(&other_path).unwrap();

            let file_path = other_path.join("inside.log");
            let move_path = other_path.join("outside.tmp");
            let sym_path = watch_path.join("sym.log");
            File::create(file_path.clone()).unwrap();
            symlink(&file_path, &sym_path).unwrap();

            let fs = Arc::new(Mutex::new(new_fs::<()>(watch_path, None)));

            rename(&file_path, &move_path).unwrap();

            take_events!(fs, 3);

            let entry = lookup_entry!(fs, sym_path);
            assert!(entry.is_some());

            let entry = lookup_entry!(fs, file_path);
            assert!(entry.is_none());

            let entry = lookup_entry!(fs, move_path);
            assert!(entry.is_none());
        });
    }

    // Watch symlink target that is excluded
    #[test]
    fn filesystem_watch_symlink_w_excluded_target() {
        run_test(|| {
            let tempdir = TempDir::new().unwrap();
            let path = tempdir.path().to_path_buf();

            let mut rules = Rules::new();
            rules.add_inclusion(GlobRule::new("*.log").unwrap());
            rules.add_inclusion(
                GlobRule::new(&*format!("{}{}", tempdir.path().to_str().unwrap(), "*")).unwrap(),
            );
            rules.add_exclusion(GlobRule::new("*.tmp").unwrap());

            let file_path = path.join("test.tmp");
            let sym_path = path.join("test.log");
            File::create(file_path.clone()).unwrap();

            let fs = Arc::new(Mutex::new(new_fs::<()>(path, Some(rules))));

            let entry = lookup_entry!(fs, file_path);
            assert!(entry.is_none());

            symlink(&file_path, &sym_path).unwrap();

            take_events!(fs, 1);

            let entry = lookup_entry!(fs, sym_path);
            assert!(entry.is_some());

            let entry = lookup_entry!(fs, file_path);
            assert!(entry.is_some());
        });
    }
}
