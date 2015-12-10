//
// Created by cse on 12/3/15.
//

#ifndef FLEXIS_TRAITS_IMPL_H
#define FLEXIS_TRAITS_IMPL_H

/////////////////////////////////////////////////////////////////////////////////////////////
// macros for inclusion in mapping implementation files. If mappings are referenced from multiple
// application files, definitions must be separated from declarations. Definitions must be kept
// in a source file. This can be achieved by using the mapping definition macros below and
// #including this file for the implementation (.cpp) file and traits_decl.h for the application
// files that should only see the declarations.
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
#define START_MAPPINGHDR_SUB(cls, sup, nm) using nm##_traits = ClassTraitsBase<cls, sup>; \
template <> struct ClassTraits<cls> : public nm##_traits {

/**
 * end the mapping header
 * @param cls the fully qualified class name
 */
#define END_MAPPINGHDR(cls) }; \
template <> const char * ClassTraitsBase<cls>::name = #cls;\
template <> ClassInfo<cls, EmptyClass> * ClassTraitsBase<cls, EmptyClass>::info = new ClassInfo<cls, EmptyClass>(#cls, typeid(cls)); \
template<> PropertyAccessBase * ClassTraitsBase<cls>::decl_props[] = {

/**
 * end the mapping header
 * @param cls the fully qualified class name
 * @param sup the name of the superclass
 */
#define END_MAPPINGHDR_SUB(cls, sup, nm) }; \
template <> const char * nm##_traits::name = #cls;\
template <> ClassInfo<cls, sup> * nm##_traits::info = ClassInfo<cls, sup>::subclass<sup>(#cls, typeid(cls)); \
template<> PropertyAccessBase * nm##_traits::decl_props[] = {

/**
 * close the mapping for one class
 * @param cls the fully qualified class name
 */
#define END_MAPPING(cls) }; \
template<> const unsigned ClassTraitsBase<cls>::decl_props_sz = ARRAY_SZ(ClassTraitsBase<cls>::decl_props); \
template<> Properties * ClassTraitsBase<cls>::properties(Properties::mk<cls>());

#define END_MAPPING_SUB(cls, sup, nm) }; \
template<> const unsigned nm##_traits::decl_props_sz = ARRAY_SZ(nm##_traits::decl_props); \
template<> Properties * nm##_traits::properties(Properties::mk<cls, sup>());

/**
 * define mapping for one property
 *
 * @param cls the fully qualified class name
 * @param propkind the mapping class name
 * @param proptype the property data type
 * @param propname the property name
 */
#define MAPPED_PROP(cls, propkind, proptype, propname) new propkind<cls, proptype, &cls::propname>(#propname),
#define MAPPED_PROP_ITER(cls, propkind, proptype, proptype2, proptype3, propname) \
new propkind<cls, proptype, proptype2<proptype>, proptype3<proptype>, &cls::propname>(#propname),

/**
 * define mapping for one property. Same as MAPPED_PROP, but with different names for property and field
 */
#define MAPPED_PROP2(cls, propkind, proptype, prop, name) new propkind<cls, proptype, &cls::prop>(#name),

#define MAPPED_PROP3(cls, propkind, proptype, propname, parm) new propkind<cls, proptype, &cls::propname>(#propname, parm),

/**
 * define mapping for a objectid property
 *
 * @param cls the fully qualified class name
 * @param the property name. The field must have type ObjectId and be initialized to 0
 */
#define OBJECT_ID(cls, prop) new ObjectIdAssign<cls, &cls::prop>(),

#endif //FLEXIS_TRAITS_IMPL_H
