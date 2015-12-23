//
// Created by cse on 12/3/15.
//

#ifndef FLEXIS_TRAITS_IMPL_H
#define FLEXIS_TRAITS_IMPL_H

#include "prop_decl.h"

///////////////////////////////////////////////////////////////////////
// horrible macro mess used to make the mapping macro language a little
// more convenient
//////////////////////////////////////////////////////////////////////

#define prop_impl(_cls, ...) template<> const PropertyAccessBase **ClassTraitsBase<_cls>::decl_props[] = \
{macro_dispatcher(prop_impl, __VA_ARGS__)(_cls, __VA_ARGS__)}
#define prop_impl_sub(_cls, sup, ...) template<> const PropertyAccessBase **ClassTraitsBase<_cls, sup>::decl_props[] = \
{macro_dispatcher(prop_impl, __VA_ARGS__)(_cls, __VA_ARGS__)}

#define CT(_c, _x) &ClassTraits<_c>::_x

#define prop_impl1(_cls, _a) \
CT(_cls,_a)
#define prop_impl2(_cls, _a, _b) \
CT(_cls,_a), CT(_cls,_b)
#define prop_impl3(_cls, _a, _b, _c) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c)
#define prop_impl4(_cls, _a, _b, _c, _d) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d)
#define prop_impl5(_cls, _a, _b, _c, _d, _e) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d), CT(_cls,_e)
#define prop_impl6(_cls, _a, _b, _c, _d, _e, _f) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d), CT(_cls,_e), CT(_cls,_f)
#define prop_impl7(_cls, _a, _b, _c, _d, _e, _f, _g) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d), CT(_cls,_e), CT(_cls,_f), CT(_cls,_g)
#define prop_impl8(_cls, _a, _b, _c, _d, _e, _f, _g, _h) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d), CT(_cls,_e), CT(_cls,_f), CT(_cls,_g), CT(_cls,_h)
#define prop_impl9(_cls, _a, _b, _c, _d, _e, _f, _g, _h, _i) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d), CT(_cls,_e), CT(_cls,_f), CT(_cls,_g), CT(_cls,_h), CT(_cls,_i)
#define prop_impl10(_cls, _a, _b, _c, _d, _e, _f, _g, _h, _i, _j) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d), CT(_cls,_e), CT(_cls,_f), CT(_cls,_g), CT(_cls,_h), CT(_cls,_i), CT(_cls,_j)
#define prop_impl11(_cls, _a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d), CT(_cls,_e), CT(_cls,_f), CT(_cls,_g), CT(_cls,_h), CT(_cls,_i), CT(_cls,_j), CT(_cls,_k)
#define prop_impl12(_cls, _a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d), CT(_cls,_e), CT(_cls,_f), CT(_cls,_g), CT(_cls,_h), CT(_cls,_i), CT(_cls,_j), CT(_cls,_k), CT(_cls,_l)
#define prop_impl13(_cls, _a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d), CT(_cls,_e), CT(_cls,_f), CT(_cls,_g), CT(_cls,_h), CT(_cls,_i), CT(_cls,_j), CT(_cls,_k), CT(_cls,_l), CT(_cls,_m)
#define prop_impl14(_cls, _a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d), CT(_cls,_e), CT(_cls,_f), CT(_cls,_g), CT(_cls,_h), CT(_cls,_i), CT(_cls,_j), CT(_cls,_k), CT(_cls,_l), CT(_cls,_m). CT(_cls,_n)
#define prop_impl15(_cls, _a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d), CT(_cls,_e), CT(_cls,_f), CT(_cls,_g), CT(_cls,_h), CT(_cls,_i), CT(_cls,_j), CT(_cls,_k), CT(_cls,_l), CT(_cls,_m). CT(_cls,_n), CT(_cls,_o)
#define prop_impl16(_cls, _a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d), CT(_cls,_e), CT(_cls,_f), CT(_cls,_g), CT(_cls,_h), CT(_cls,_i), CT(_cls,_j), CT(_cls,_k), CT(_cls,_l), CT(_cls,_m). CT(_cls,_n), CT(_cls,_o), CT(_cls,_p)
#define prop_impl17(_cls, _a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d), CT(_cls,_e), CT(_cls,_f), CT(_cls,_g), CT(_cls,_h), CT(_cls,_i), CT(_cls,_j), CT(_cls,_k), CT(_cls,_l), CT(_cls,_m). CT(_cls,_n), CT(_cls,_o), CT(_cls,_p), CT(_cls,_q)
#define prop_impl18(_cls, _a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q, _r) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d), CT(_cls,_e), CT(_cls,_f), CT(_cls,_g), CT(_cls,_h), CT(_cls,_i), CT(_cls,_j), CT(_cls,_k), CT(_cls,_l), CT(_cls,_m). CT(_cls,_n), CT(_cls,_o), CT(_cls,_p), CT(_cls,_q), CT(_cls,_r)
#define prop_impl19(_cls, _a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q, _r, _s) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d), CT(_cls,_e), CT(_cls,_f), CT(_cls,_g), CT(_cls,_h), CT(_cls,_i), CT(_cls,_j), CT(_cls,_k), CT(_cls,_l), CT(_cls,_m). CT(_cls,_n), CT(_cls,_o), CT(_cls,_p), CT(_cls,_q), CT(_cls,_r), CT(_cls,_s)
#define prop_impl20(_cls, _a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q, _r, _s, _t) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d), CT(_cls,_e), CT(_cls,_f), CT(_cls,_g), CT(_cls,_h), CT(_cls,_i), CT(_cls,_j), CT(_cls,_k), CT(_cls,_l), CT(_cls,_m). CT(_cls,_n), CT(_cls,_o), CT(_cls,_p), CT(_cls,_q), CT(_cls,_r), CT(_cls,_s), CT(_cls,_t)
#define prop_impl21(_cls, _a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q, _r, _s, _t, _u) \
CT(_cls,_a), CT(_cls,_b), CT(_cls,_c), CT(_cls,_d), CT(_cls,_e), CT(_cls,_f), CT(_cls,_g), CT(_cls,_h), CT(_cls,_i), CT(_cls,_j), CT(_cls,_k), CT(_cls,_l), CT(_cls,_m). CT(_cls,_n), CT(_cls,_o), CT(_cls,_p), CT(_cls,_q), CT(_cls,_r), CT(_cls,_s), CT(_cls,_t), CT(_cls,_u)

/////////////////////////////////////////////////////////////////////////////////////////////
// macros for inclusion in mapping implementation files. If mappings are referenced from multiple
// application files, definitions must be separated from declarations. Definitions must be kept
// in a source file. This can be achieved by using the mapping definition macros below and
// #including this file for the implementation (.cpp) file and traits_decl.h for the application
// files that should only see the declarations.
////////////////////////////////////////////////////////////////////////////////////////////

/**
 * start the mapping
 *
 * @param _cls the fully qualified class name
 * @param list of all maped property names
 */
#define START_MAPPING(_cls, ...) template <> struct ClassTraits<_cls> : \
public ClassTraitsBase<_cls>, public ClassTraitsConcrete<_cls>{ \
static const PropertyAccessBase prop_decl(__VA_ARGS__) ;}; \
template<> bool ClassTraitsBase<_cls>::traits_initialized = false; \
template<> const unsigned ClassTraitsBase<_cls>::num_decl_props = VA_NUM_ARGS(__VA_ARGS__); \
prop_impl(_cls, __VA_ARGS__);

/**
 * start the mapping for an abstract class
 *
 * @param _cls the fully qualified class name
 * @param list of all maped property names
 */
#define START_MAPPING_A(_cls, ...) template <> struct ClassTraits<_cls> : \
public ClassTraitsBase<_cls>, public ClassTraitsAbstract<_cls>{ \
static const PropertyAccessBase prop_decl(__VA_ARGS__) ;}; \
template<> bool ClassTraitsBase<_cls>::traits_initialized = false; \
template<> const unsigned ClassTraitsBase<_cls>::num_decl_props = VA_NUM_ARGS(__VA_ARGS__); \
prop_impl(_cls, __VA_ARGS__);

/**
 * start the mapping for a class with a superclass
 *
 * @param _cls the fully qualified class name
 * @param _sup the fully qualified name of the superclass
 * @param list of all maped property names
 */
#define START_MAPPING_SUB(_cls, _sup, ...) \
template <> struct ClassTraits<_cls> : public ClassTraitsBase<_cls, _sup>, public ClassTraitsConcrete<_cls> { \
static const PropertyAccessBase prop_decl(__VA_ARGS__) ;}; \
template<> bool ClassTraitsBase<_cls, _sup>::traits_initialized = false; \
template<> const unsigned ClassTraitsBase<_cls, _sup>::num_decl_props = VA_NUM_ARGS(__VA_ARGS__); \
prop_impl_sub(_cls, _sup, __VA_ARGS__);

/**
 * start the mapping for an abstract class with a superclass
 *
 * @param _cls the fully qualified class name
 * @param _sup the fully qualified name of the superclass
 * @param list of all maped property names
 */
#define START_MAPPING_SUB_A(_cls, _sup, ...) \
template <> struct ClassTraits<_cls> : public ClassTraitsBase<_cls, _sup>, public ClassTraitsAbstract<_cls> { \
static const PropertyAccessBase prop_decl(__VA_ARGS__) ;}; \
template<> bool ClassTraitsBase<_cls, _sup>::traits_initialized = false; \
template<> const unsigned ClassTraitsBase<_cls, _sup>::num_decl_props = VA_NUM_ARGS(__VA_ARGS__); \
prop_impl_sub(_cls, _sup, __VA_ARGS__);

/**
 * end the mapping header
 * @param cls the fully qualified class name
 */
#define END_MAPPING(cls) template <> const char * ClassTraitsBase<cls>::traits_classname = #cls;\
template <> ClassInfo<cls, EmptyClass> * ClassTraitsBase<cls, EmptyClass>::traits_info = new ClassInfo<cls, EmptyClass>(#cls, typeid(cls)); \
template<> Properties * ClassTraitsBase<cls>::traits_properties(PropertiesImpl<EmptyClass>::mk<cls>());

/**
 * end the mapping header
 * @param cls the fully qualified class name
 * @param sup the name of the superclass
 */
#define END_MAPPING_SUB(cls, sup) template <> const char * ClassTraitsBase<cls, sup>::traits_classname = #cls;\
template <> ClassInfo<cls, sup> * ClassTraitsBase<cls, sup>::traits_info = ClassInfo<cls, sup>::subclass<sup>(#cls, typeid(cls)); \
template<> Properties * ClassTraitsBase<cls, sup>::traits_properties(PropertiesImpl<sup>::mk<cls>());

/**
 * define mapping for one property
 *
 * @param cls the fully qualified class name
 * @param propkind the mapping class name
 * @param proptype the property data type
 * @param propname the property name
 */
#define MAPPED_PROP(cls, propkind, proptype, propname) \
const PropertyAccessBase *ClassTraits<cls>::propname = new propkind<cls, proptype, &cls::propname>(#propname);

/**
 * define mapping for one iterator-type property
 *
 * @param cls the fully qualified class name
 * @param propkind the mapping class name
 * @param proptype the iterated-over data type
 * @param proptype2 the declared type of the iterator property (object that implements the iterator)
 * @param proptype3 the KV-store-aware replacement type for the iterator property (subclasses proptype2)
 * @param propname the property name
 */
#define MAPPED_PROP_ITER(cls, propkind, proptype, proptype2, proptype3, propname) \
const PropertyAccessBase *ClassTraits<cls>::propname = \
new propkind<cls, proptype, proptype2<proptype>, proptype3<proptype>, &cls::propname>(#propname);

/**
 * define mapping for one property. Same as MAPPED_PROP, but with different names for property and field
 */
#define MAPPED_PROP2(cls, propkind, proptype, prop, name) \
const PropertyAccessBase *ClassTraits<cls>::prop = new propkind<cls, proptype, &cls::prop>(#name);

/**
 * define mapping for one property. Same as MAPPED_PROP, but with an additional parameter that is pased to the
 * property accessor
 */
#define MAPPED_PROP3(cls, propkind, proptype, propname, parm) \
const PropertyAccessBase *ClassTraits<cls>::propname = new propkind<cls, proptype, &cls::propname>(#propname, parm);

/**
 * define mapping for a objectid property
 *
 * @param cls the fully qualified class name
 * @param the property name. The field must have type ObjectId and be initialized to 0
 */
#define OBJECT_ID(cls, propname) \
const PropertyAccessBase *ClassTraits<cls>::propname = new ObjectIdAssign<cls, &cls::propname>();

#endif //FLEXIS_TRAITS_IMPL_H
