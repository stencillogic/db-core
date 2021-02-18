//! Build &str using existing [u8] array
/*
use std;

pub struct StrBuilder<'a> {
    buf: &'a mut [u8],
    written: usize,
}

impl<'a> StrBuilder<'a> {
    pub fn new(buf: &'a mut [u8]) -> Self {
        StrBuilder {
            buf,
            written: 0
        }
    }

    pub fn add(mut self, chunk: &str) -> Self {
        let src = chunk.as_bytes();
        let dst = &mut self.buf[self.written..self.written + src.len()];
        dst.copy_from_slice(src);
        self.written += src.len();
        self
    }

    pub fn get_str(self) -> Result<&'a str, std::str::Utf8Error> {
        std::str::from_utf8(self.buf)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_build() {

        let expected = "Hello, World!";

        let mut buf = [0; 13];

        let s = StrBuilder::new(&mut buf)
            .add("Hello")
            .add(", ")
            .add("World")
            .add("!")
            .add("")
            .get_str();

        assert_eq!(s.unwrap(), expected);
    }

    #[test]
    #[should_panic]
    fn test_overflow() {

        let expected = "Hello, World!";

        let mut buf = [0; 13];

        let s = StrBuilder::new(&mut buf)
            .add("Hello")
            .add(", ")
            .add("World")
            .add("!")
            .add("a")
            .get_str();

        assert_eq!(s.unwrap(), expected);
    }

    #[test]
    fn test_empty() {

        let expected = "";

        let mut buf = [0; 0];

        let s = StrBuilder::new(&mut buf)
            .add("")
            .add("")
            .add("")
            .get_str();

        assert_eq!(s.unwrap(), expected);

        let s = StrBuilder::new(&mut buf)
            .get_str();

        assert_eq!(s.unwrap(), expected);
    }

    #[test]
    fn test_incomplete() {

        let expected = "Hello";

        let mut buf = [0; 13];

        let s = StrBuilder::new(&mut buf)
            .add("Hello")
            .get_str();

        assert_eq!(&s.unwrap()[..5], expected);
    }
}
*/
