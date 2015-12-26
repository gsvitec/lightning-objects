//
// Created by cse on 12/3/15.
//

/////////////////////////////////////////////////////////////////////////////////////////////
// declaration-only macros for inclusion in application files. If mappings are referenced from multiple
// application files, definitions must be separated from declarations. Definitions must be kept in a source
// file. This can be achieved by using the mapping definition macros below and #including this file for the
// application files and traits_impl.h for the mapping definition file
////////////////////////////////////////////////////////////////////////////////////////////
#include "prop_decl.h"

/** @see header traits_impl.h */
#define START_MAPPING(_cls, ...) template <> struct ClassTraits<_cls> : \
public ClassTraitsBase<_cls>, public ClassTraitsConcrete<_cls>{ \
static const PropertyAccessBase prop_decl(__VA_ARGS__) ;};

/** @see header traits_impl.h */
#define START_MAPPING_A(_cls, ...) template <> struct ClassTraits<_cls> : \
public ClassTraitsBase<_cls>, public ClassTraitsAbstract<_cls>{ \
static const PropertyAccessBase prop_decl(__VA_ARGS__) ;};

/** @see header traits_impl.h */
#define START_MAPPING_SUB(_cls, _sup, ...) \
template <> struct ClassTraits<_cls> : public ClassTraitsBase<_cls, _sup>, public ClassTraitsConcrete<_cls> { \
static const PropertyAccessBase prop_decl(__VA_ARGS__) ;};

/** @see header traits_impl.h */
#define START_MAPPING_SUB_A(_cls, _sup, ...) \
template <> struct ClassTraits<_cls> : public ClassTraitsBase<_cls, _sup>, public ClassTraitsAbstract<_cls> { \
static const PropertyAccessBase prop_decl(__VA_ARGS__) ;};

/** @see header traits_impl.h */
#define END_MAPPING(_cls, ...)

/** @see header traits_impl.h */
#define END_MAPPING_SUB(_cls, sub, ...)

/** @see header traits_impl.h */
#define MAPPED_PROP(_cls, propkind, proptype, propname)

/** @see header traits_impl.h */
#define MAPPED_PROP_ITER(_cls, propkind, proptype, proptype2, proptype3, propname)

/** @see header traits_impl.h */
#define MAPPED_PROP2(_cls, propkind, proptype, prop, name)

/** @see header traits_impl.h */
#define MAPPED_PROP3(_cls, propkind, proptype, propname, parm)

/** @see header traits_impl.h */
#define OBJECT_ID(_cls, prop)
