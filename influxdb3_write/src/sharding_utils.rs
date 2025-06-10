// Copyright (c) 2024 InfluxData Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use std::hash::Hasher;
use twox_hash::XxHash64;

// A fixed seed for the hash function to ensure deterministic output.
// This specific value is arbitrary; any constant u64 would work.
const HASH_SEED: u64 = 0xDEADBEEFBAADF00D;

/// Calculates a 64-bit hash for a slice of string-like values.
///
/// The values are concatenated and then hashed using XxHash64 with a fixed seed
/// to ensure deterministic behavior.
///
/// # Arguments
///
/// * `values`: A slice of items that can be referenced as strings (e.g., `&str`, `String`).
///
/// # Returns
///
/// A `u64` representing the calculated hash.
pub fn calculate_hash(values: &[impl AsRef<str>]) -> u64 {
    let mut hasher = XxHash64::with_seed(HASH_SEED);
    for value in values {
        hasher.write(value.as_ref().as_bytes());
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_hash_empty() {
        let values: &[&str] = &[];
        let hash = calculate_hash(values);
        // Expected value is the hash of an empty string with the given seed.
        // Can be pre-calculated: XxHash64::with_seed(HASH_SEED).finish()
        let mut expected_hasher = XxHash64::with_seed(HASH_SEED);
        let expected_hash = expected_hasher.finish();
        assert_eq!(hash, expected_hash);
    }

    #[test]
    fn test_calculate_hash_single_value() {
        let values = ["hello"];
        let hash = calculate_hash(&values);

        let mut expected_hasher = XxHash64::with_seed(HASH_SEED);
        expected_hasher.write(b"hello");
        assert_eq!(hash, expected_hasher.finish());
    }

    #[test]
    fn test_calculate_hash_multiple_values() {
        let values = ["hello", "world"];
        let hash = calculate_hash(&values);

        let mut expected_hasher = XxHash64::with_seed(HASH_SEED);
        expected_hasher.write(b"hello");
        expected_hasher.write(b"world");
        assert_eq!(hash, expected_hasher.finish());
    }

    #[test]
    fn test_calculate_hash_different_order_yields_different_hash() {
        let values1 = ["hello", "world"];
        let hash1 = calculate_hash(&values1);

        let values2 = ["world", "hello"];
        let hash2 = calculate_hash(&values2);

        assert_ne!(hash1, hash2, "Hashes should differ for different orderings if concatenation implies order");

        // Verify explicit concatenation for clarity on what's being hashed
        let mut hasher1_concat = XxHash64::with_seed(HASH_SEED);
        hasher1_concat.write(b"helloworld"); // Simulate direct concatenation if that was the intent (it is not, it's sequential write)
        // The current calculate_hash writes sequentially, not as a single concatenated string.
        // So this test is more about the properties of sequential hashing.
    }

    #[test]
    fn test_calculate_hash_consistency() {
        let values = ["consistency_check", "123!@#"];
        let hash1 = calculate_hash(&values);
        let hash2 = calculate_hash(&values);
        assert_eq!(hash1, hash2, "Hash should be consistent for the same input.");
    }

    #[test]
    fn test_calculate_hash_with_strings() {
        let values = [String::from("test"), String::from("case")];
        let hash = calculate_hash(&values);

        let mut expected_hasher = XxHash64::with_seed(HASH_SEED);
        expected_hasher.write(b"test");
        expected_hasher.write(b"case");
        assert_eq!(hash, expected_hasher.finish());
    }
}
