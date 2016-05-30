# Lightning Objects
## C++ Object Storage based on LMDB Key/Value Store
Lightning Objects (LO, henceforth) is a C++ object mapping layer that interfaces to an
abstract transaction-based key/value API for actual storage. The abstract API is currently implemented for LMDB
(https://symas.com/products/lightning-memory-mapped-database/) only.

LO is currently targeted at in-process embedded deployment and maintains a single dedicated database file. However,
since LMDB has multi-process support, this can easily be implemented in LO at a later time. There are currently no plans
for a remote protocol - but again, LO is mostly a mapping layer, and properties of the underlying platform can
easily be exposed.
## Features
LO supports (single) inheritance and polymorphism

LO provides flexible mapping options that range from scalar values to multi-valued
object relationships. Mappings can be combined with different storage strategies, for example:
 - embedded storage fully serializes all mapped members into one object buffer
 - keyed storage stores every member into a separate, standalone key slot

It also offers a macro-based mapping language that makes it easy to describe how your objects
are saved into the KV store.

Here is a short mapping example:

~~~{.cpp}
START_MAPPING(flexis::Overlays::Colored2DPoint, x, y, r, g, b, a)
    MAPPED_PROP(flexis::Overlays::Colored2DPoint, ValuePropertyEmbeddedAssign, float, x)
    MAPPED_PROP(flexis::Overlays::Colored2DPoint, ValuePropertyEmbeddedAssign, float, y)
    MAPPED_PROP(flexis::Overlays::Colored2DPoint, ValuePropertyEmbeddedAssign, float, r)
    MAPPED_PROP(flexis::Overlays::Colored2DPoint, ValuePropertyEmbeddedAssign, float, g)
    MAPPED_PROP(flexis::Overlays::Colored2DPoint, ValuePropertyEmbeddedAssign, float, b)
    MAPPED_PROP(flexis::Overlays::Colored2DPoint, ValuePropertyEmbeddedAssign, float, a)
END_MAPPING(flexis::Overlays::Colored2DPoint)
~~~

LO provides a convenient object-oriented API. Most runtime interaction is with transaction objects
which wrap the underlying data store transactions:

~~~{.cpp}
ObjectKey key;
{
    Colored2DPoint p;
    p.set(2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.5f);

    auto wtxn = kv->beginWrite();
    wtxn->saveObject(p, key);
    wtxn->commit();
}
{
    Colored2DPoint *p2;
    auto rtxn = kv->beginRead();
    p2 = rtxn->getObject<Colored2DPoint>(key);
    rtxn->end();
}
~~~
## Status
LO is currently in alpha state. This means that there are features that are not fully tested (but quite a few
are, as you can see in the test subdirectory), and API changes might still happen. However, LO has already proven
to be useful and will be improved in the near future into a fully production-ready object storage solution.
## License
LO is licensed under the GNU Lesser General Public License, Version 3.0
## More Information..
.. can be gathered from the accompanying documentation and from the test/demo programs (lmdb/test).
Don't forget to try the lo_dump tool which allows you to inspect the contents of a LO
database interactively
