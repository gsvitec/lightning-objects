//
// Created by cse on 12/3/15.
//
/**
 * start the mapping header.
 * @param cls the fully qualified class name
 */
#define START_MAPPINGHDR(cls) template <> struct ClassTraits<cls> : public ClassTraitsBase<cls>{

/**
 * start the mapping header.
 * @param cls the fully qualified class name
 */
#define START_MAPPINGHDR_SUB(cls, sup, nm) template <> struct ClassTraits<cls> : public ClassTraitsBase<cls, sup> {

#define END_MAPPINGHDR(cls) };
#define END_MAPPINGHDR_SUB(cls, sup, nm) };
#define END_MAPPING(cls)
#define END_MAPPING_SUB(cls, sub, nm)

#define MAPPED_PROP(cls, propkind, proptype, propname)
#define MAPPED_PROP2(cls, propkind, proptype, prop, name)
#define MAPPED_PROP3(cls, propkind, proptype, propname, parm)
#define OBJECT_ID(cls, prop)
