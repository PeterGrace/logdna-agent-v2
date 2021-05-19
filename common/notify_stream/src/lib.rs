extern crate notify;

use futures::{stream, Stream};
use notify::{
    DebouncedEvent, Error as NotifyError, RecommendedWatcher, RecursiveMode,
    Watcher as NotifyWatcher,
};
use std::io;
use std::path::Path;
use std::sync::mpsc::{channel, Receiver};
use std::time::Duration;

type PathId = std::path::PathBuf;

#[derive(Debug)]
/// Event wrapper to that hides platform and implementation details.
///
/// Gives us the ability to hide/map events from the used library and minimize code changes in
/// case the notify library adds breaking changes.
pub enum Event {
    /// `NoticeWrite` is emitted immediately after the first write event for the path.
    ///
    /// If you are reading from that file, you should probably close it immediately and discard all
    /// data you read from it.
    NoticeWrite(PathId),

    /// `NoticeRemove` is emitted immediately after a remove or rename event for the path.
    ///
    /// The file will continue to exist until its last file handle is closed.
    NoticeRemove(PathId),

    /// `Create` is emitted when a file or directory has been created and no events were detected
    /// for the path within the specified time frame.
    ///
    /// `Create` events have a higher priority than `Write` and `Chmod`. These events will not be
    /// emitted if they are detected before the `Create` event has been emitted.
    Create(PathId),

    /// `Write` is emitted when a file has been written to and no events were detected for the path
    /// within the specified time frame.
    ///
    /// `Write` events have a higher priority than `Chmod`. `Chmod` will not be emitted if it's
    /// detected before the `Write` event has been emitted.
    ///
    /// Upon receiving a `Create` event for a directory, it is necessary to scan the newly created
    /// directory for contents. The directory can contain files or directories if those contents
    /// were created before the directory could be watched, or if the directory was moved into the
    /// watched directory.
    Write(PathId),

    /// `Remove` is emitted when a file or directory has been removed and no events were detected
    /// for the path within the specified time frame.
    Remove(PathId),

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
    Error(Option<PathId>),
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
    watcher: RecommendedWatcher,
    rx: Receiver<DebouncedEvent>,
}

impl Watcher {
    pub fn new() -> Self {
        let (tx, rx) = channel();

        let watcher = NotifyWatcher::new(tx, Duration::from_secs(1)).unwrap();
        Self { watcher, rx }
    }

    /// Adds a new directory to watch
    pub fn add<P: AsRef<Path>>(&mut self, path: P) -> Result<(), Error> {
        self.watcher
            .watch(path, RecursiveMode::Recursive)
            .map_err(|e| match e {
                NotifyError::Generic(s) => Error::Generic(s),
                NotifyError::Io(err) => Error::Io(err),
                NotifyError::PathNotFound => Error::PathNotFound,
                NotifyError::WatchNotFound => Error::WatchNotFound,
            })
    }

    pub fn receive(&self) -> impl Stream<Item = Event> + '_ {
        stream::unfold(&self.rx, |rx| async move {
            loop {
                match rx.recv() {
                    Ok(event) => {
                        if let Some(mapped_event) = match event {
                            DebouncedEvent::NoticeWrite(p) => Some(Event::NoticeWrite(p)),
                            DebouncedEvent::NoticeRemove(p) => Some(Event::NoticeRemove(p)),
                            DebouncedEvent::Create(p) => Some(Event::Create(p)),
                            DebouncedEvent::Write(p) => Some(Event::Write(p)),
                            DebouncedEvent::Chmod(_) => {
                                // Ignore attribute changes
                                None
                            }
                            DebouncedEvent::Remove(p) => Some(Event::Remove(p)),
                            DebouncedEvent::Rename(source, dest) => {
                                Some(Event::Rename(source, dest))
                            }
                            DebouncedEvent::Rescan => Some(Event::Rescan),
                            DebouncedEvent::Error(_, p) => Some(Event::Error(p)),
                        } {
                            return Some((mapped_event, rx));
                        }
                    }
                    Err(_) => {
                        return Some((Event::Error(None), rx));
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::fs::File;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_watch_file() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();

        let mut w = Watcher::new();
        w.add(dir_path).unwrap();

        File::create(dir_path.join("insert.log")).unwrap();

        let mut stream = w.receive();
        let items =
            futures::StreamExt::collect::<Vec<_>>(futures::StreamExt::take(stream, 1)).await;
        for event in items {
            println!("{:?}", event);
        }
    }
}
