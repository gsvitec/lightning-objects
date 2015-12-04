//
// Created by cse on 12/3/15.
//

#ifndef FLEXIS_TRAITS_IMPL_H
#define FLEXIS_TRAITS_IMPL_H

#if defined (_WIN32) && !defined(FLEXISKV_NODLL)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/**
 * end the mapping header
 * @param cls the fully qualified class name
 */
#define END_MAPPINGHDR(cls) }; template<> EXPORT ClassInfo ClassTraitsBase<cls>::info (#cls, typeid(cls)); \
template<> EXPORT PropertyAccessBase * ClassTraitsBase<cls>::decl_props[] = {

/**
 * end the mapping header with inheritance
 * @param cls the fully qualified class name
 */
#define END_MAPPINGHDR_INH(cls, base) }; template<> EXPORT ClassInfo base::info (#cls, typeid(cls)); \
template<> EXPORT PropertyAccessBase * base::decl_props[] = {

/**
 * close the mapping for one class
 * @param cls the fully qualified class name
 */
#define END_MAPPING(cls) }; \
template<> EXPORT const unsigned ClassTraitsBase<cls>::decl_props_sz = ARRAY_SZ(ClassTraits<cls>::decl_props); \
template<> EXPORT Properties * ClassTraitsBase<cls>::properties(Properties::mk<cls>());

/**
 * close the mapping for one class, with inheritance
 * @param cls the fully qualified class name
 * @param sup the fully qualified name of the superclass
 */
#define END_MAPPING_INH2(cls, base, sup) }; \
template<> EXPORT const unsigned base::decl_props_sz = ARRAY_SZ(base::decl_props); \
template<> EXPORT Properties * base::properties(Properties::mk<cls, sup>());

#define END_MAPPING_INH(cls, base) }; \
template<> EXPORT const unsigned base::decl_props_sz = ARRAY_SZ(base::decl_props); \
template<> EXPORT Properties * base::properties(Properties::mk<cls>());

/**
 * define mapping for one property
 *
 * @param cls the fully qualified class name
 * @param propkind the mapping class name
 * @param proptype the property data type
 * @param propname the property name
 */
#define MAPPED_PROP(cls, propkind, proptype, propname) new propkind<cls, proptype, &cls::propname>(#propname),

/**
 * define mapping for one property. Same as MAPPED_PROP, but with different names for property and field
 */
#define MAPPED_PROP2(cls, propkind, proptype, prop, name) new propkind<cls, proptype, &cls::prop>(#name),

/**
 * define mapping for a objectid property
 *
 * @param cls the fully qualified class name
 * @param the property name. The field must have type ObjectId and be initialized to 0
 */
#define OBJECT_ID(cls, prop) new ObjectIdAssign<cls, &cls::prop>(),

#endif //FLEXIS_TRAITS_IMPL_H
