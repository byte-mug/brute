# brute

An eventually consistent database and a testground for replication/clustering techniques.

# Technology

Somehow inspired by Amazon Dynamo and Amazon SimpleDB

The core idea is to accumulate write operations which are idempotent and commutative,
and to make an eventually consistent datastore with those operations. Those operations
are represented as Items.

## Items

In Brute, an Item is a write operation and a record at the same time (excluding
the Key). All write operations are required to be idempotent and commutative.

Brute is designed to store Key-Item-Pairs. Pairs with the same Key are merged,
requiring these merge operations to be idempotent and commutative.

### LWW (Last Write Wins) Key-Value store

An example is the Timestamp-based Last-Write-Wins

```
key => (timestamp,TRUE,value)
key => (timestamp,FALSE)

```

