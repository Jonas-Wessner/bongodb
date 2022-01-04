///
/// `TryConvertAllExt` is an extension trait that marks that an implementor of the trait provides
/// a function to try to convert all its items to another type using a `converter` function.
///
/// `TryConvertAll` is typically implemented for collection.
/// `try_convert_all` then tries to convert all items of type `T1` into another type `T2`.
/// If successful, which means if `converter` returns Ok(T2) for all items, `Ok(Vec<T2>)` is returned.
/// If the converter Function returns an `Err` for one item, the conversion is immediately stopped
/// and the `Err` is returned to the caller.
/// Therefore `try_convert_all` is fail-fast, which safes performance in Err-cases.
/// Furthermore `try_convert_all` takes `self` as an argument and consumes it.
///
pub trait TryConvertAllExt<F: Fn(T1) -> Result<T2, E>, T1, T2, E> {
    fn try_convert_all(self, converter: F) -> Result<Vec<T2>, E>;
}

impl<C: AsMut<[T1]> + Into<Vec<T1>>, F: Fn(T1) -> Result<T2, E>, T1, T2, E> TryConvertAllExt<F, T1, T2, E> for C {
    fn try_convert_all(self, converter: F) -> Result<Vec<T2>, E> {
        let mut result: Vec<T2> = vec![];
        let mut items = self.into();
        // consume items one after another to be able to move, leaving the vector empty
        for _ in 0..items.len() {
            // fail early and bubble up first error if one occurs
            result.push(converter(items.remove(0))?);
        }

        Ok(result)
    }
}


///
/// `TryConvertOption` is an extension trait that can be implemented by option types to mark that
/// they provide a function `try_convert_option` which tries to convert the containing value of the
/// option with a given `converter` function.
///
/// If the conversion of the contained value returns an `Err`, it is returned to the caller of
/// `try_convert_option`.
/// If the conversion is successful, meaning it returns an `Ok(T2)` a `Ok(Option<T2>)` is returned.
///
pub trait TryConvertOption<F: Fn(T1) -> Result<T2, E>, T1, T2, E> {
    fn try_convert_option(self, converter: F) -> Result<Option<T2>, E>;
}

impl<F: Fn(T1) -> Result<T2, E>, T1, T2, E> TryConvertOption<F, T1, T2, E> for Option<T1> {
    fn try_convert_option(self, converter: F) -> Result<Option<T2>, E> {
        match self {
            Some(t1) => {Ok(Some(converter(t1)?))},
            None => { Ok(None) }
        }
    }
}
