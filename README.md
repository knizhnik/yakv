**YAKV** is very simple persistent-key value storage implemented in Rust
using "traditional" architecture: B-Tree, buffer cache, ACID transaction, write-ahead log.
**YAKV** implements simple MURSIW (multiple-reads-single-writer) access pattern
and is first of all oriented on embedded applications.

It has minimal dependencies from other modules and contains just 1500 lines of code.

