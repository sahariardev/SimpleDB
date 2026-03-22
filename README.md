# SimpleDB

A lightweight, transactional key-value database engine implemented in Java. SimpleDB uses a B+ Tree for indexing, a Buffer Pool for memory management, and a Write-Ahead Log (WAL) to ensure data integrity and crash recovery.

---

## Features

### B+ Tree Indexing
Efficiently stores and retrieves keys using a balanced tree structure, supporting search, insertion, and deletion operations.

### Buffer Pool Management
Implements an LRU (Least Recently Used) caching strategy to manage data pages in memory, minimizing disk I/O.

### ACID Transactions
Supports `begin()`, `commit()`, and `abort()` operations to ensure atomic updates.

### Crash Recovery
Uses a Write-Ahead Log (WAL) to recover the database to a consistent state after an unexpected shutdown.

### Persistence
Automatically flushes data to disk, ensuring information is preserved across sessions.

---

## Project Structure

- **BPlusTree.java**  
  The core indexing engine managing `InternalNode` and `LeafNode` structures.

- **BufferPool.java**  
  Handles page caching, eviction policies, and disk synchronization.

- **WAL.java**  
  Manages the Write-Ahead Log to record all database changes before they are applied.

- **Page.java**  
  Represents a fixed-size unit of data storage (4KB) containing key-value pairs.

- **SimpleDB.java**  
  The high-level API providing an interface for database operations and transaction management.

- **Main.java**  
  A demonstration class showing basic CRUD operations, transaction handling, and crash recovery.

---

## Getting Started

### Prerequisites

- Java 17 or higher
- Gradle 8.12 (Wrapper included)

### Building the Project

Use the provided Gradle wrapper to build the application:

```bash
./gradlew build
```

### Running the Demo

You can run the Main class to see the database in action:

```bash
./gradlew run
```

### Basic Usage

```java
String dbPath = "./mydb";
SimpleDB db = new SimpleDB(dbPath);

// Basic CRUD
db.put("name", "Alice");
String value = db.get("name"); // Returns "Alice"
db.delete("name");

// Transactions
db.begin();
db.put("balance", "1000");
db.commit(); // Changes are persisted

// Aborting a Transaction
db.begin();
db.put("balance", "2000");
db.abort(); // Changes are rolled back; balance remains "1000"

db.close();
```

### How It Works
1. Storage
Data is organized into Page objects. When a page exceeds its capacity, the BufferPool manages its persistence to the dataDir.

2. Indexing
The BPlusTree maintains a mapping between keys and their corresponding pageId. This allows the database to locate specific data without scanning every file on disk.

3. Recovery
On startup, the recover() method reads the wal.log. It identifies committed transactions and re-applies their changes while ignoring any uncommitted or aborted writes, ensuring the database remains in a valid state.