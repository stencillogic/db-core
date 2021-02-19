# DB-Core 

![Rust](https://github.com/stencillogic/db-core/workflows/Rust/badge.svg)

Current status: WIP

DB-Core is a transaction processing system with ACID guaranties for client application's data. 
It can be used as a backend for a DBMS, or a no-sql database, or as a sort of embedded dbms.


### Usage

Client application creates a so called instance of db-core. The instance then can be cloned to make things multithreaded.
When the first instance is created it initializes data store, shared block buffer, log manager, and restores state from the last checkpoint.
Using created instance client application then can start transaction, create or open existing object, read and write data, delete objects, and then commit or rollback changes.
Finally, application can shutdown all instances.
