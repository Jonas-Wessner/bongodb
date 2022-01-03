// TODO: documentation

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
