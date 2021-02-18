//! 
//! File system utility functions
//!


use crate::common::errors::Error;
use std::fs::DirEntry;
use std::path::Path;


/// traverse directory and call callback on each entry (dirs, files, links, etc.) 
pub fn traverse_dir<T>(path: &Path, include_subdirs: bool, mut callback: T) -> Result<(), Error> 
    where T: FnMut(&DirEntry) -> Result<(), Error> {

    let mut stack = Vec::new();
    let mut dir = path.to_path_buf();

    loop {
        for entry in std::fs::read_dir(dir)? {

            let entry = entry?;
            let entry_path = entry.path();

            if entry_path.is_dir() && include_subdirs {
                stack.push(entry_path);
            }

            callback(&entry)?;
        }

        if let Some(entry_path) = stack.pop() {
            dir = entry_path;
        } else {
            break;
        }
    }

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    fn prepare_dir_tree(dirs: &[&str], files: &[&str]) {
        for item in dirs  {
            std::fs::create_dir_all(item).unwrap();
        }
        for item in files {
            std::fs::write(item, "test").unwrap();
        }
    }

    fn cleanup(dirs: &[&str], files: &[&str]) {
        for item in files {
            std::fs::remove_file(item).unwrap();
        }
        for item in dirs  {
            std::fs::remove_dir_all(item).unwrap();
        }
    }

    #[test]
    fn traverse_test() {

        let dirs = ["dir_890232676743234/subdir1/subsub", 
            "dir_890232676743234/subdir2"];
        let files = ["dir_890232676743234/test1.file",
            "dir_890232676743234/test2.file",
            "dir_890232676743234/subdir1/test1.file",
            "dir_890232676743234/subdir2/test1.file"];

        prepare_dir_tree(&dirs, &files);


        // traverse with subdirs
        let mut result = std::collections::HashSet::<String>::new();

        traverse_dir(std::path::Path::new("dir_890232676743234"), true, |entry: &DirEntry| -> Result<(), Error> {
            result.insert(String::from(entry.path().to_str().unwrap()));
            Ok(())
        }).unwrap();

        let mut expected = std::collections::HashSet::<String>::new();
        expected.insert(String::from("dir_890232676743234/subdir1"));
        for item in &dirs {
            expected.insert(String::from(*item));
        }
        for item in &files {
            expected.insert(String::from(*item));
        }

        assert_eq!(expected, result);


        // traverse excluding subdirs
        let mut result = std::collections::HashSet::<String>::new();

        traverse_dir(std::path::Path::new("dir_890232676743234"), false, |entry: &DirEntry| -> Result<(), Error> {
            result.insert(String::from(entry.path().to_str().unwrap()));
            Ok(())
        }).unwrap();

        let mut expected = std::collections::HashSet::<String>::new();
        expected.insert(String::from("dir_890232676743234/subdir1"));
        expected.insert(String::from("dir_890232676743234/subdir2"));
        expected.insert(String::from("dir_890232676743234/test1.file"));
        expected.insert(String::from("dir_890232676743234/test2.file"));

        assert_eq!(expected, result);


        cleanup(&["dir_890232676743234"], &files);
    }

}
