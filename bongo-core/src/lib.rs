#![feature(iter_intersperse)]

pub mod bongo_request;
pub mod bongo_response;
pub mod serialize;
pub mod types;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
