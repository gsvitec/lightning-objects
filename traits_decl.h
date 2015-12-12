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

/** @see header traits_impl.h */
#define START_MAPPINGHDR(cls) template <> struct ClassTraits<cls> : \
public ClassTraitsBase<cls>, public ClassTraitsConcrete<cls>{

/** @see header traits_impl.h */
#define START_MAPPINGHDR_A(cls) template <> struct ClassTraits<cls> : \
public ClassTraitsBase<cls>, public ClassTraitsAbstract<cls>{

/** @see header traits_impl.h */
#define START_MAPPINGHDR_SUB(cls, sup, nm) template <> struct ClassTraits<cls> : \
public ClassTraitsBase<cls, sup>, public ClassTraitsConcrete<cls> {

/** @see header traits_impl.h */
#define START_MAPPINGHDR_SUB_A(cls, sup, nm) template <> struct ClassTraits<cls> : \
public ClassTraitsBase<cls, sup>, public ClassTraitsAbstract<cls> {

/** @see header traits_impl.h */
#define END_MAPPINGHDR(cls) };

/** @see header traits_impl.h */
#define END_MAPPINGHDR_SUB(cls, sup, nm) };

/** @see header traits_impl.h */
#define END_MAPPING(cls)

/** @see header traits_impl.h */
#define END_MAPPING_SUB(cls, sub, nm)

/** @see header traits_impl.h */
#define MAPPED_PROP(cls, propkind, proptype, propname)

/** @see header traits_impl.h */
#define MAPPED_PROP_ITER(cls, propkind, proptype, proptype2, proptype3, propname)

/** @see header traits_impl.h */
#define MAPPED_PROP2(cls, propkind, proptype, prop, name)

/** @see header traits_impl.h */
#define MAPPED_PROP3(cls, propkind, proptype, propname, parm)

/** @see header traits_impl.h */
#define OBJECT_ID(cls, prop)
