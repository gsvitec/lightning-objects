//
// Created by cse on 12/3/15.
//

#ifndef FLEXIS_TRAITS_DECL_H
#define FLEXIS_TRAITS_DECL_H

/**
 * end the mapping header
 * @param cls the fully qualified class name
 */
#define END_MAPPINGHDR(cls) };

/**
 * end the mapping header with inheritance
 * @param cls the fully qualified class name
 */
#define END_MAPPINGHDR_INH(cls, base) };

/**
 * close the mapping for one class
 * @param cls the fully qualified class name
 */
#define END_MAPPING(cls)

/**
 * close the mapping for one class, with inheritance
 * @param cls the fully qualified class name
 * @param sup the fully qualified name of the superclass
 */
#define END_MAPPING_INH2(cls, base, sup)
#define END_MAPPING_INH(cls, base)

/**
 * define mapping for one property
 *
 * @param cls the fully qualified class name
 * @param propkind the mapping class name
 * @param proptype the property data type
 * @param propname the property name
 */
#define MAPPED_PROP(cls, propkind, proptype, propname)

/**
 * define mapping for one property. Same as MAPPED_PROP, but with different names for property and field
 */
#define MAPPED_PROP2(cls, propkind, proptype, prop, name)

/**
 * define mapping for a objectid property
 *
 * @param cls the fully qualified class name
 * @param the property name. The field must have type ObjectId and be initialized to 0
 */
#define OBJECT_ID(cls, prop)

#endif //FLEXIS_TRAITS_DECL_H
