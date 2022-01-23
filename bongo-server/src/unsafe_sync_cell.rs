use std::cell::UnsafeCell;

///
/// `UnsafeSyncCell` has two main advantages that are given by leveraging unsafe code:
/// 1. It gives mutable access to its contents while itself can be immutable.
/// This is just the behavior of RefCell.
/// 2. It implements Send (not like RefCell) and can therefore be passed to different threads.
///
/// This is useful when the behavior of RefCell is needed with multithreading and it can be ensured
/// that the contained type T is Sync.
/// Normally we would use Arc<Mutex<T>>, but then we would have to lock the entire object when accessing it.
/// If T implements a better more efficient way of synchronizing accesses, it would be hindering to use Mutex.
///
/// In the BongoDB project `UnsafeSyncCell` is used to capture a mutable reference to an `Executor`
/// in an `Fn` closure used as the request handler of the webserver. As `Executor` implements concurrency
/// more efficiently than Mutex<Executor> would by internally using RwLock only for what really needs
/// to be locked, we use `UnsafeSyncCell` to be able to use that under rusts borrow checking rules.
///
pub(crate) struct UnsafeSyncCell<T> {
    cell: UnsafeCell<T>,
}

impl<T> UnsafeSyncCell<T> {
    pub fn new(ex: T) -> Self {
        Self { cell: UnsafeCell::new(ex) }
    }

    pub fn get(&self) -> &mut T {
        unsafe { &mut *self.cell.get() }
    }
}

unsafe impl<T> Send for UnsafeSyncCell<T> where T: Sync {}

unsafe impl<T> Sync for UnsafeSyncCell<T> where T: Sync {}