//
// Created by cse on 12/3/15.
//

/////////////////////////////////////////////////////////////////////////////////////////////
// macros for inclusion in application files. If mappings are referenced from multiple
// application files, definitions must be separated from declarations. Definitions must be kept
// in a source file. This can be achieved by using the mapping definition macros below and
// #including this file for the application files and traits_impl.h for the mapping definition
// file
////////////////////////////////////////////////////////////////////////////////////////////

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
