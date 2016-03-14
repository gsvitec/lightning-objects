//
// Created by cse on 12/22/15.
//
#include <type_traits>

#define BEGIN_KV_NS namespace flexis { namespace persistence { namespace kv {
#define END_KV_NS }}}

#ifdef _MSC_VER
#define VA_NUM_ARGS(...) VA_NUM_ARGS_IMPL_((__VA_ARGS__, 26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1))
#define VA_NUM_ARGS_IMPL_(tuple) VA_NUM_ARGS_IMPL tuple
#define VA_NUM_ARGS_IMPL(_1,_2,_3,_4,_5,_6,_7,_8,_9,_10,_11,_12,_13,_14,_15,_16,_17,_18,_19,_20,_21,_22,_23,_24,_25,_26,N,...) N

#define macro_dispatcher(macro, ...) macro_dispatcher_(macro, VA_NUM_ARGS(__VA_ARGS__))
#define macro_dispatcher_(macro, nargs) macro_dispatcher__(macro, nargs)
#define macro_dispatcher__(macro, nargs) macro_dispatcher___(macro, nargs)
#define macro_dispatcher___(macro, nargs) macro ## nargs

#else

#define VA_NUM_ARGS(...) VA_NUM_ARGS_IMPL(__VA_ARGS__, 26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1)
#define VA_NUM_ARGS_IMPL(_1,_2,_3,_4,_5,_6,_7,_8,_9,_10,_11,_12,_13,_14,_15,_16,_17,_18,_19,_20,_21,_22,_23,_24,_25,_26,N,...) N

#define macro_dispatcher(_func, ...) macro_dispatcher_(_func, VA_NUM_ARGS(__VA_ARGS__))
#define macro_dispatcher_(_func, _nargs) macro_dispatcher__(_func, _nargs)
#define macro_dispatcher__(_func, _nargs) _func ## _nargs
#endif

#define prop_decl(...) macro_dispatcher(prop_decl, __VA_ARGS__)(__VA_ARGS__);
#define prop_decl1(_a) *_a
#define prop_decl2(_a, _b) *_a, *_b
#define prop_decl3(_a, _b, _c) *_a, *_b, *_c
#define prop_decl4(_a, _b, _c, _d) *_a, *_b, *_c, *_d
#define prop_decl5(_a, _b, _c, _d, _e) *_a, *_b, *_c, *_d, *_e
#define prop_decl6(_a, _b, _c, _d, _e, _f) *_a, *_b, *_c, *_d, *_e, *_f
#define prop_decl7(_a, _b, _c, _d, _e, _f, _g) *_a, *_b, *_c, *_d, *_e, *_f, *_g
#define prop_decl8(_a, _b, _c, _d, _e, _f, _g, _h) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h
#define prop_decl9(_a, _b, _c, _d, _e, _f, _g, _h, _i) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i
#define prop_decl10(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j
#define prop_decl11(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k
#define prop_decl12(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l
#define prop_decl13(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m
#define prop_decl14(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m, *_n
#define prop_decl15(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m, *_n, *_o
#define prop_decl16(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m, *_n, *_o, *_p
#define prop_decl17(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m, *_n, *_o, *_p, *_q
#define prop_decl18(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q, _r) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m, *_n, *_o, *_p, *_q, *_r
#define prop_decl19(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q, _r, _s) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m, *_n, *_o, *_p, *_q, *_r, *_s
#define prop_decl20(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q, _r, _s, _t) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m, *_n, *_o, *_p, *_q, *_r, *_s, *_t
#define prop_decl21(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q, _r, _s, _t, _u) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m, *_n, *_o, *_p, *_q, *_r, *_s, *_t, *_u
#define prop_decl22(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q, _r, _s, _t, _u, _v) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m, *_n, *_o, *_p, *_q, *_r, *_s, *_t, *_u, *_v
#define prop_decl23(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q, _r, _s, _t, _u, _v, _w) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m, *_n, *_o, *_p, *_q, *_r, *_s, *_t, *_u, *_v, *_w
#define prop_decl24(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q, _r, _s, _t, _u, _v, _w, _x) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m, *_n, *_o, *_p, *_q, *_r, *_s, *_t, *_u, *_v, *_w, *_x
#define prop_decl25(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q, _r, _s, _t, _u, _v, _w, _x, _y) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m, *_n, *_o, *_p, *_q, *_r, *_s, *_t, *_u, *_v, *_w, *_x, *_y
#define prop_decl26(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q, _r, _s, _t, _u, _v, _w, _x, _y, _z) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m, *_n, *_o, *_p, *_q, *_r, *_s, *_t, *_u, *_v, *_w, *_x, *_y, *_z;
