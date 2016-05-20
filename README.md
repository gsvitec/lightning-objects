#Lightning Objects
##C++ Object Storage based on LMDB Key/Value Store
Lightning Objects (LO, henceforth) is a C++ object mapping layer that interfaces to an
abstract transaction-based key/value API for actual storage. The abstract API is currently implemented for LMDB
(https://symas.com/products/lightning-memory-mapped-database/) only.

LO is currently targeted at in-process embedded deployment and maintains a single database file. However, since
LMDB has multi-process support, this can also be easily implemented in LO at a later time.
##Features
LO provides flexible mapping options that range from scalar values to multi-valued
object relationships. Mappings can be combined with different storage strategies:
 - embedded storage fully serializes mapped members into the main buffer
 - keyed storage stores every member into a separate, standalone key slot

It also offers a macro-based mapping language that makes it easy to describe how your objects
are saved into the KV store.

LO provides a convenient object-oriented API. Most runtime interaction is with transaction objects
which wrap the underlying store (LMDB) transactions.
##Status
LO is currently in alpha state. This means that there are features that are not fully tested, and
documentation is partly outdated and mostly incomplete. However, LO has already proven to be useful
and will be improved in the near future into a fully production-ready object storage solution.
##More Information..
.. can be gathered from the (currently outdated) documentation and from the test/demo programs (lmdb/test).
Don't forget to try the lo_dump tool which allows you to inspect the contents of a LO
database interactively
