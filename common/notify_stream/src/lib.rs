extern crate notify;

use futures::{stream, Stream};
use notify::{DebouncedEvent, Error as NotifyError, RecursiveMode, Watcher as NotifyWatcher};
use std::io;
use std::path::Path;
use std::rc::Rc;
use std::time::Duration;

type PathId = std::path::PathBuf;

#[cfg(target_os = "linux")]
type OsWatcher = notify::INotifyWatcher;
#[cfg(target_os = "windows")]
type OsWatcher = notify::ReadDirectoryChangesWatcher;
#[cfg(not(any(target_os = "linux", target_os = "windows")))]
type OsWatcher = notify::PollWatcher;

#[derive(Debug)]
/// Event wrapper to that hides platform and implementation details.
///
/// Gives us the ability to hide/map events from the used library and minimize code changes in
/// case the notify library adds breaking changes.
pub enum Event {
    /// `NoticeRemove` is emitted immediately after a remove or rename event for the path.
    ///
    /// The file will continue to exist until its last file handle is closed.
    ///
    /// `Write` events might follow as part of the normal flow.
    Remove(PathId),

    /// `Create` is emitted when a file or directory has been created and no events were detected
    /// for the path within the specified time frame.
    ///
    /// `Create` events have a higher priority than `Write`, `Write` will not be
    /// emitted if they are detected before the `Create` event has been emitted.
    Create(PathId),

    /// `Write` is emitted when a file has been written to and no events were detected for the path
    /// within the specified time frame.
    ///
    /// Upon receiving a `Create` event for a directory, it is necessary to scan the newly created
    /// directory for contents. The directory can contain files or directories if those contents
    /// were created before the directory could be watched, or if the directory was moved into the
    /// watched directory.
    Write(PathId),

    /// `Rename` is emitted when a file or directory has been moved within a watched directory and
    /// no events were detected for the new path within the specified time frame.
    ///
    /// The first path contains the source, the second path the destination.
    Rename(PathId, PathId),

    /// `Rescan` is emitted immediately after a problem has been detected that makes it necessary
    /// to re-scan the watched directories.
    Rescan,

    /// `Error` is emitted immediately after a error has been detected.
    ///
    ///  This event may contain a path for which the error was detected.
    Error(Error, Option<PathId>),
}

#[derive(Debug)]
pub enum Error {
    /// Generic error
    ///
    /// May be used in cases where a platform specific error is mapped to this type
    Generic(String),

    /// I/O errors
    Io(io::Error),

    /// The provided path does not exist
    PathNotFound,

    /// Attempted to remove a watch that does not exist
    WatchNotFound,
}

pub struct Watcher {
    watcher: OsWatcher,
    rx: Rc<async_channel::Receiver<DebouncedEvent>>,
}

impl Watcher {
    pub fn new(delay: Duration) -> Self {
        let (watcher_tx, blocking_rx) = std::sync::mpsc::channel();

        let watcher = OsWatcher::new(watcher_tx, delay).unwrap();
        let (async_tx, rx) = async_channel::unbounded();
        tokio::task::spawn_blocking(move || {
            while let Ok(event) = blocking_rx.recv() {
                async_tx.try_send(event).expect("channel can not be closed");
            }
        });

        Self {
            watcher,
            rx: Rc::new(rx),
        }
    }

    /// Adds a new directory or file to watch
    pub fn watch<P: AsRef<Path>>(&mut self, path: P) -> Result<(), Error> {
        self.watcher
            .watch(path, RecursiveMode::Recursive)
            .map_err(|e| e.into())
    }

    /// Removes a file or directory
    pub fn unwatch<P: AsRef<Path>>(&mut self, path: P) -> Result<(), Error> {
        self.watcher.unwatch(path).map_err(|e| e.into())
    }

    /// Starts receiving the watcher events
    pub fn receive(&self) -> impl Stream<Item = Event> {
        let rx = Rc::clone(&self.rx);
        stream::unfold(rx, |rx| async move {
            loop {
                let received = rx.recv().await.expect("channel can not be closed");
                if let Some(mapped_event) = match received {
                    DebouncedEvent::NoticeRemove(p) => Some(Event::Remove(p)),
                    DebouncedEvent::Create(p) => Some(Event::Create(p)),
                    DebouncedEvent::Write(p) => Some(Event::Write(p)),
                    DebouncedEvent::Rename(source, dest) => Some(Event::Rename(source, dest)),
                    // TODO: Define what to do with Rescan
                    DebouncedEvent::Rescan => Some(Event::Rescan),
                    DebouncedEvent::Error(e, p) => Some(Event::Error(e.into(), p)),
                    // NoticeWrite can be useful but we don't use it
                    DebouncedEvent::NoticeWrite(_) => None,
                    // Ignore `Remove`: we use `NoticeRemove` that comes before in the flow
                    DebouncedEvent::Remove(_) => None,
                    // Ignore attribute changes
                    DebouncedEvent::Chmod(_) => None,
                } {
                    return Some((mapped_event, rx));
                }
            }
        })
    }
}

impl From<notify::Error> for Error {
    fn from(e: notify::Error) -> Error {
        match e {
            NotifyError::Generic(s) => Error::Generic(s),
            NotifyError::Io(err) => Error::Io(err),
            NotifyError::PathNotFound => Error::PathNotFound,
            NotifyError::WatchNotFound => Error::WatchNotFound,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::StreamExt;
    use pin_utils::pin_mut;
    use std::cell::RefCell;
    use std::fs::File;
    use std::io::{self, Write};
    use tempfile::tempdir;

    static DELAY: Duration = Duration::from_millis(200);

    macro_rules! is_match {
        ($p: expr, $e: ident, $expected_path: expr) => {
            match $p {
                Event::$e(path) => {
                    assert_eq!(path.file_name(), $expected_path.file_name());
                    assert_eq!(
                        path.parent().unwrap().file_name(),
                        $expected_path.parent().unwrap().file_name()
                    );
                }
                _ => panic!("event didn't match Event::{}", stringify!($e)),
            }
        };
    }

    macro_rules! take {
        ($stream: ident, $result: ident) => {
            tokio::time::sleep(DELAY).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            loop {
                tokio::select! {
                    item = $stream.next() => {
                        $result.push(item.unwrap());
                    }
                    _ = tokio::time::sleep(Duration::from_millis(200)) => {
                        break;
                    }
                }
            }
        };
    }

    macro_rules! append {
        ($file: ident) => {
            for i in 0..20 {
                writeln!($file, "SAMPLE {}", i)?;
            }
        };
    }

    macro_rules! wait_and_append {
        ($file: ident) => {
            tokio::time::sleep(DELAY.clone().mul_f32(3.0)).await;
            append!($file);
        };
    }

    #[tokio::test]
    async fn test_initial_write_get_debounced_into_create() -> io::Result<()> {
        let dir = tempdir().unwrap().into_path();
        let dir_path = &dir;

        let mut w = Watcher::new(DELAY);
        w.watch(dir_path).unwrap();

        let file1_path = dir_path.join("file1.log");
        let mut file1 = File::create(&file1_path)?;
        append!(file1);

        let stream = w.receive();
        pin_mut!(stream);

        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut items = Vec::new();
        take!(stream, items);
        // Depending on timers, it will get debounced or not :(
        assert!(!items.is_empty());
        is_match!(&items[0], Create, file1_path);
        Ok(())
    }

    #[tokio::test]
    async fn test_create_write_delete() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();

        let mut w = Watcher::new(DELAY);
        w.watch(dir_path).unwrap();

        let file_path = dir_path.join("file1.log");
        let mut file = File::create(&file_path)?;
        append!(file);

        let stream = w.receive();
        pin_mut!(stream);

        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut items = Vec::new();
        take!(stream, items);
        // Depending on timers, it can get debounced into a single create
        assert!(!items.is_empty());
        is_match!(&items[0], Create, file_path);

        wait_and_append!(file);
        std::fs::remove_file(&file_path)?;
        take!(stream, items);

        let is_equal = |p: &PathId| p.as_os_str() == file_path.as_os_str();
        let items: Vec<_> = items
            .iter()
            .filter(|e| match e {
                Event::Write(p) => is_equal(p),
                Event::Remove(p) => is_equal(p),
                Event::Create(p) => is_equal(p),
                _ => false,
            })
            .collect();

        is_match!(items.last().unwrap(), Remove, file_path);

        Ok(())
    }

    #[tokio::test]
    async fn test_watch_file_write_after_create() -> io::Result<()> {
        let dir = tempdir().unwrap().into_path();

        let mut w = Watcher::new(DELAY);
        w.watch(&dir).unwrap();

        let file1_path = &dir.join("file1.log");
        let mut file1 = File::create(&file1_path)?;

        let stream = w.receive();
        pin_mut!(stream);

        let mut items = Vec::new();
        take!(stream, items);

        assert!(!items.is_empty());
        is_match!(&items[0], Create, file1_path);

        wait_and_append!(file1);
        take!(stream, items);

        is_match!(&items[1], Write, file1_path);
        Ok(())
    }

    /// Must add watch to file target to work on both linux and macOS
    #[tokio::test]
    #[cfg(unix)]
    async fn test_watch_symlink_write_after_create() -> io::Result<()> {
        let dir = tempdir().unwrap().into_path();
        let excluded_dir = tempdir().unwrap().into_path();

        let w = RefCell::new(Watcher::new(DELAY));
        {
            let mut w_mut = w.borrow_mut();
            w_mut.watch(&dir).unwrap();
        }

        let file_path = &excluded_dir.join("file1.log");
        let symlink_path = &dir.join("symlink.log");
        let mut file = File::create(&file_path)?;
        std::os::unix::fs::symlink(&file_path, &symlink_path)?;

        {
            let w_ref = w.borrow();
            let stream = w_ref.receive();
            pin_mut!(stream);

            let mut items = Vec::new();
            take!(stream, items);

            assert!(!items.is_empty());
            is_match!(&items[0], Create, symlink_path);
        }

        {
            let mut w_mut = w.borrow_mut();
            w_mut.watch(&file_path).unwrap();
        }

        wait_and_append!(file);

        tokio::time::sleep(Duration::from_millis(1000)).await;

        {
            let w_ref = w.borrow();
            let stream = w_ref.receive();
            pin_mut!(stream);

            let mut items = Vec::new();
            take!(stream, items);

            // macOS will produce events for both the symlink and the file
            // linux will produce events for the real file manually added
            let items: Vec<_> = items
                .iter()
                .filter(|e| match e {
                    Event::Write(p) => p.as_os_str() == file_path.as_os_str(),
                    _ => false,
                })
                .collect();

            is_match!(&items[0], Write, file_path);
        }
        Ok(())
    }
}
