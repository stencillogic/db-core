# DB-Core

DB-Core is a transaction processing system with ACID guaranties for client application's data. 
It can be used as a backend for a DBMS, or a no-sql database, or as a sort of embedded dbms.

## Introduction

DB-core allow client applications to create, read, write and delete objects. Object is simply a sequence of bytes. Each object has a unique id associated with it.
CRUD operations on objects are transactional. Currently supported isolation level for transactions is read committed. 

DB-Core provides storage driver. By default data is stored on persisten storage organized by blocks in the file system, but storage driver is not limited by this organization.

In order to recover from failures DB-Core keeps track of all changes by recording them in the transaction log. 
Current implementation supports storing logs in files as well but not limited with this implementation. 
To speed things up on start after failure checkpointing is included in the storage driver implementation. 
In case of failure just the log records written after the last checkpoint are replayed.

BC-Core uses memory buffer for the data store blocks shared by all client threads. The default eviction mechanism is LRU, but other kind can be implemented as well.

Basic MVCC is implemented as part of storage dirver. It represents versioning store for recording older versions of the data while it is changed by a certain transaction. 
Versioned data is used in case of transaction rollback, and for reading by transactions if data is being modified and dirty.

### Usage

Client application creates a so called instance of db-core. The instance then can be cloned to make things multithreaded.
When the first instance is created it initialized data store, shared block buffer, log manager, and restores statefrom the last checkpoint.
Ising instance client application then can start transaction, create or open existing object, read and write data, delete objects, and then commit or rollback changes.
Finally, application can shutdown all instances.
