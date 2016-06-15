/*
 * LightningObjects C++ Object Storage based on Key/Value API
 *
 * Copyright (C) 2016 GS Vitec GmbH <christian@gsvitec.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, and provided
 * in the LICENSE file in the root directory of this software.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#ifndef TRAITS_DECL_INCLUDED
#define TRAITS_DECL_INCLUDED

/////////////////////////////////////////////////////////////////////////////////////////////
// declaration-only macros for inclusion in application files. If mappings are referenced from multiple
// application files, definitions must be separated from declarations. Definitions must be kept in a source
// file. This can be achieved by using the mapping definition macros below and #including this file for the
// application files and traits_impl.h for the mapping definition file
////////////////////////////////////////////////////////////////////////////////////////////
#include "prop_decl.h"
#include "traits_undef.h"

/** @see header traits_impl.h */
#define START_MAPPING(_cls, ...) template <> struct ClassTraits<_cls> : \
public ClassTraitsBase<_cls>, public ClassTraitsConcrete<_cls>{ \
static const PropertyAccessBase prop_decl(__VA_ARGS__) ;};

/** @see header traits_impl.h */
#define START_MAPPING_REPL(_cls, _repl, ...) template <> struct ClassTraits<_cls> : \
public ClassTraitsBase<_cls>, public ClassTraitsConcreteRepl<_cls, _repl>{ \
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
#define START_MAPPING_SUB_REPL(_cls, _repl, _sup, ...) \
template <> struct ClassTraits<_cls> : public ClassTraitsBase<_cls, _sup>, public ClassTraitsConcreteRepl<_cls, _repl> { \
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

/** @see header traits_impl.h */
#define KV_TYPEDEF(__type, __bytes, __isCont) template <> struct TypeTraits<__type> {\
static ClassId id; static const char *name; static const unsigned byteSize=__bytes; static const bool isVect=__isCont;\
};

#endif //TRAITS_DECL_INCLUDED
