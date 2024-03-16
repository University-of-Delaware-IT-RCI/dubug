/*!
 * dubug - [du] [b]y [u]ser and [g]roup
 *
 * API for managing simple byte buffers (sbb).
 *
 */

#ifndef __SBB_H__
#define __SBB_H__

#include "config.h"

typedef struct sbb * sbb_ref;

enum {
    sbb_options_immutable = 1 << 0,
    sbb_options_external_buffer = 1 << 1,
    sbb_options_byte_swap = 1 << 2
};

sbb_ref sbb_alloc(size_t capacity, unsigned int options);
sbb_ref sbb_alloc_with_const_buffer(unsigned int options, const void *p, size_t p_len);
sbb_ref sbb_alloc_with_bytes(size_t capacity, unsigned int options, const void *p, size_t p_len);

void sbb_free(sbb_ref a_buffer);

void sbb_summary(sbb_ref a_buffer);

size_t sbb_get_capacity(sbb_ref a_buffer);
bool sbb_set_capacity(sbb_ref a_buffer, size_t capacity);

size_t sbb_get_length(sbb_ref a_buffer);
bool sbb_set_length(sbb_ref a_buffer, size_t length, uint8_t fill_byte);
void sbb_reset(sbb_ref a_buffer);

const void* sbb_get_buffer_ptr(sbb_ref a_buffer);

bool sbb_append_buffer(sbb_ref a_buffer, const void *p, size_t p_len);
bool sbb_append_multiple_buffers(sbb_ref a_buffer, const void *p, size_t p_len, ...);
bool sbb_append_cstring(sbb_ref a_buffer, const char *cstr);
bool sbb_append_uint8(sbb_ref a_buffer, uint8_t v);
bool sbb_append_uint16(sbb_ref a_buffer, uint16_t v);
bool sbb_append_uint32(sbb_ref a_buffer, uint32_t v);
bool sbb_append_uint64(sbb_ref a_buffer, uint64_t v);

typedef struct sbb_deserialize_state {
    sbb_ref     a_buffer;
    size_t      idx;
} sbb_deserialize_state_t;

sbb_deserialize_state_t* sbb_deserialize_start(sbb_ref a_buffer, sbb_deserialize_state_t *sbb_state);

bool sbb_deserialize_buffer(sbb_deserialize_state_t *sbb_state, void **p, size_t *p_len);
bool sbb_deserialize_cstring(sbb_deserialize_state_t *sbb_state, char **cstr);
bool sbb_deserialize_uint8(sbb_deserialize_state_t *sbb_state, uint8_t *v);
bool sbb_deserialize_uint16(sbb_deserialize_state_t *sbb_state, uint16_t *v);
bool sbb_deserialize_uint32(sbb_deserialize_state_t *sbb_state, uint32_t *v);
bool sbb_deserialize_uint64(sbb_deserialize_state_t *sbb_state, uint64_t *v);

/*
 * A deserialization block can easily be generated using the following set of
 * macros.
 *
 * The block is opened with the SBB_DESERIALIZE_BEGIN macro.  This sets-up the field
 * counter and failure state variables on the local stack.  The field counter is
 * named with a "__" prefix, the buffer object's name, and a "_field" suffix and is
 * an int.
 *
 * Next, a sequence of deserialize actions are performed.  If a deserialize error
 * occurs, execution will jump to the SBB_DESERIALIZE_ON_ERROR block.
 *
 * After all deserialize actions, the SBB_DESERIALIZE_ON_ERROR block is provided to
 * react to errors.  The __<name>_field indicates which deserialize action failed.
 *
 * Next is the SBB_DESERIALIZE_OTHERWISE block that executes if all actions were
 * successful.
 *
 * Finally, the deserialization block is ended with SBB_DESERIALIZE_END.
 *
 * For example, given an sbb object named "our_data":
 *
 *     uint16_t      i1, i2;
 *     uint32_t      l1;
 *     const char    *cstr;
 *
 *     SBB_DESERIALIZE_BEGIN(our_data)
 *         SBB_DESERIALIZE_UINT16(our_data, i1);
 *         SBB_DESERIALIZE_UINT16(our_data, i2);
 *         SBB_DESERIALIZE_CSTRING(our_data, cstr);
 *         SBB_DESERIALIZE_UINT32(our_data, l1);
 *    SBB_DESERIALIZE_ON_ERROR(our_data)
 *         printf("Failed deserialization on field %d.\n", __our_data_field);
 *    SBB_DESERIALIZE_OTHERWISE(our_data)
 *         printf("%04hX %04hX \"%s\" %08X\n", i1, i2, cstr, l1);
 *    SBB_DESERIALIZE_END(our_data)
 *
 */
#define SBB_DESERIALIZE_BEGIN(X)            while ( 1 ) { \
                                                bool __##X##_okay = false; \
                                                int  __##X##_field = 0; \
                                                while ( 1 ) { \
                                                    sbb_deserialize_state_t __##X##_state; \
                                                    sbb_deserialize_start((X), &__##X##_state);

#define SBB_DESERIALIZE_BUFFER(X, P, P_LEN)         if ( ! sbb_deserialize_buffer(&__##X##_state, (void**)&(P), &(P_LEN)) ) break; __##X##_field++;
#define SBB_DESERIALIZE_CSTRING(X, CSTR)            if ( ! sbb_deserialize_cstring(&__##X##_state, &(CSTR)) ) break; __##X##_field++;
#define SBB_DESERIALIZE_UINT8(X, V)                 if ( ! sbb_deserialize_uint8(&__##X##_state, &(V)) ) break; __##X##_field++;
#define SBB_DESERIALIZE_UINT16(X, V)                if ( ! sbb_deserialize_uint16(&__##X##_state, &(V)) ) break; __##X##_field++;
#define SBB_DESERIALIZE_UINT32(X, V)                if ( ! sbb_deserialize_uint32(&__##X##_state, &(V)) ) break; __##X##_field++;
#define SBB_DESERIALIZE_UINT64(X, V)                if ( ! sbb_deserialize_uint64(&__##X##_state, &(V)) ) break; __##X##_field++;

#define SBB_DESERIALIZE_ON_ERROR(X)                 __##X##_okay = true; \
                                                    break; \
                                                } \
                                                if ( ! __##X##_okay ) {
#define SBB_DESERIALIZE_OTHERWISE(X)                break; \
                                                } else {
#define SBB_DESERIALIZE_END(X)                  } \
                                                break; \
                                            }
                                                



#endif /* __SBB_H__ */
