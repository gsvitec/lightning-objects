//
// Created by cse on 12/22/15.
//

#define VA_NUM_ARGS(...) VA_NUM_ARGS_IMPL(__VA_ARGS__, 21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1)
#define VA_NUM_ARGS_IMPL(_1,_2,_3,_4,_5,_6,_7,_8,_9,_10,_11,_12,_13,_14,_15,_16,_17,_18,_19,_20,_21,N,...) N

#define macro_dispatcher(func, ...) macro_dispatcher_(func, VA_NUM_ARGS(__VA_ARGS__))
#define macro_dispatcher_(func, _nargs) macro_dispatcher__(func, _nargs)
#define macro_dispatcher__(func, _nargs) func ## _nargs

#define prop_decl(...) macro_dispatcher(prop_decl, __VA_ARGS__)(__VA_ARGS__) ;
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
#define prop_decl14(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m. *_n
#define prop_decl15(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m. *_n, *_o
#define prop_decl16(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m. *_n, *_o, *_p
#define prop_decl17(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m. *_n, *_o, *_p, *_q
#define prop_decl18(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q, _r) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m. *_n, *_o, *_p, *_q, *_r
#define prop_decl19(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q, _r, _s) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m. *_n, *_o, *_p, *_q, *_r, *_s
#define prop_decl20(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q, _r, _s, _t) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m. *_n, *_o, *_p, *_q, *_r, *_s, *_t
#define prop_decl21(_a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n, _o, _p, _q, _r, _s, _t, _u) *_a, *_b, *_c, *_d, *_e, *_f, *_g, *_h, *_i, *_j, *_k, *_l, *_m. *_n, *_o, *_p, *_q, *_r, *_s, *_t, *_u
