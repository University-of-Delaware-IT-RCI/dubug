/*!
 * dubug - [du] [b]y [u]ser and [g]roup
 *
 * API for managing simple byte buffers (sbb).
 *
 */

#include "sbb.h"
#include "simple_verbose_logging.h"
#include <endian.h>

//

#define SBB_USER_OPTIONS (sbb_options_byte_swap)

//

typedef struct sbb {
    size_t          capacity, length;
    unsigned int    options;
    void            *buffer;
} sbb_t;

//

size_t
__sbb_next_capacity(
    size_t      capacity
)
{
    static size_t   capacities_list[] = {
            24,
            48,
            64,
            128,
            256,
            384,
            512,
            768,
            1024,
            2048,
            4096,
            0
        };
    size_t          alt_capacity, *capacities = capacities_list;
    
    while ( *capacities && (capacity > *capacities) ) capacities++;
    if ( *capacities ) return *capacities;
    alt_capacity = capacity << 1;
    return ( (alt_capacity < capacity) ? SIZE_MAX : alt_capacity );
}

//

sbb_ref
sbb_alloc(
    size_t          capacity,
    unsigned int    options
)
{
    sbb_t       *new_obj = malloc(sizeof(sbb_t));
    
    if ( new_obj ) {
        memset(new_obj, 0, sizeof(sbb_t));
        new_obj->options = options & SBB_USER_OPTIONS;
        if ( capacity > 0 ) {
            new_obj->buffer = malloc(capacity);
            if ( new_obj->buffer ) {
                new_obj->capacity = capacity;
            } else {
                //printf("sbb_alloc\n");
                free(new_obj);
                new_obj = NULL;
            }
        }
    }
    return (sbb_ref)new_obj;
}

//

sbb_ref
sbb_alloc_with_const_buffer(
    unsigned int    options,
    const void      *p,
    size_t          p_len
)
{
    sbb_t       *new_obj = malloc(sizeof(sbb_t));
    
    if ( new_obj ) {
        new_obj->capacity = new_obj->length = p_len;
        new_obj->options = (sbb_options_immutable | sbb_options_external_buffer) | (options & SBB_USER_OPTIONS);
        new_obj->buffer = (void*)p;
    }
    return new_obj;
}

//

sbb_ref
sbb_alloc_with_bytes(
    size_t          capacity,
    unsigned int    options,
    const void      *p,
    size_t          p_len
)
{
    sbb_ref     new_obj = sbb_alloc(capacity, options);
    
    if ( new_obj ) sbb_append_buffer(new_obj, p, p_len);
    return new_obj;
}

//

void
sbb_free(
    sbb_ref     a_buffer
)
{
    if ( a_buffer ) {
        if ( ! (a_buffer->options & sbb_options_external_buffer) && a_buffer->buffer ) {
            //fprintf(stderr, "sbb_free %p\n", a_buffer->buffer);
            free(a_buffer->buffer);
        }
        //fprintf(stderr, "sbb_free 2 %p\n", a_buffer);
        free(a_buffer);
    }
}

//

void
sbb_summary(
    sbb_ref     a_buffer
)
{
    size_t      i = 0, i_max = a_buffer->length;
    uint8_t     *p = a_buffer->buffer;
    char        *out_buffer_ptr, out_buffer[1024];
    size_t      out_buffer_len;
    
    svl_printf(verbosity_debug, "sbb@%p <capacity=%llu,length=%llu,options=0x%02X> {\n", a_buffer, a_buffer->capacity, a_buffer->length, a_buffer->options);
    while ( i < i_max ) {
        char    chars[17];
        int     l = 0, n;
        
        out_buffer_ptr = out_buffer;
        out_buffer_len = sizeof(out_buffer);
        
        n = snprintf(out_buffer_ptr, out_buffer_len, "    %08X : ", i);
        out_buffer_ptr += n, out_buffer_len -= n;
        
        while ( (l < 16) && (i < i_max) ) {
            if ( l == 8 ) {
                n = snprintf(out_buffer_ptr, out_buffer_len, "   ");
                out_buffer_ptr += n, out_buffer_len -= n;
            }
            n = snprintf(out_buffer_ptr, out_buffer_len, "%02X ", *p);
            out_buffer_ptr += n, out_buffer_len -= n;
            
            chars[l] = (isprint(*p) ? *p : '.');
            l++, p++, i++;
        }
        chars[l] = '\0';
        while ( l < 16 ) {
            n = snprintf(out_buffer_ptr, out_buffer_len, (l == 8) ? "      " : "   ");
            out_buffer_ptr += n, out_buffer_len -= n;
            l++;
        }
        n = snprintf(out_buffer_ptr, out_buffer_len, "   %s", chars);
        svl_printf(verbosity_debug, "%s", out_buffer);
    }
    svl_printf(verbosity_debug, "}");
}

//

size_t
sbb_get_capacity(
    sbb_ref     a_buffer
)
{
    return a_buffer->capacity;
}

//

bool
sbb_set_capacity(
    sbb_ref     a_buffer,
    size_t      capacity
)
{
    if ( (a_buffer->options & (sbb_options_immutable | sbb_options_external_buffer)) ) return false;
    
    if ( capacity <= 0 ) capacity = __sbb_next_capacity(a_buffer->capacity);
    if ( capacity > a_buffer->capacity ) {
        void    *p = realloc(a_buffer->buffer, capacity);
        
        //fprintf(stderr, "sbb_set_capacity => %p\n", p);
        if ( ! p ) return false;
        a_buffer->buffer = p;
        a_buffer->capacity = capacity;
    }
    return true;
}

//

size_t
sbb_get_length(
    sbb_ref     a_buffer
)
{
    return a_buffer->length;
}

//

bool
__sbb_set_length(
    sbb_ref     a_buffer,
    size_t      length
)
{
    if ( length > a_buffer->length ) {
        if ( length > a_buffer->capacity ) {
            if ( (a_buffer->options & sbb_options_immutable) ) return false;
            if ( ! sbb_set_capacity(a_buffer, __sbb_next_capacity(a_buffer->length + length)) ) return false;
        }
    }
    a_buffer->length = length;
    return true;
}

bool
sbb_set_length(
    sbb_ref     a_buffer,
    size_t      length,
    uint8_t     fill_byte
)
{
    bool        is_okay, do_fill = ( length > a_buffer->length );
    
    if ( (is_okay = __sbb_set_length(a_buffer, length)) && do_fill ) {
        memset(a_buffer->buffer + a_buffer->length, fill_byte, length - a_buffer->length);
    }
    return is_okay;
}

//

void
sbb_reset(
    sbb_ref a_buffer
)
{
   sbb_set_length(a_buffer, 0, 0); 
}

//

const void*
sbb_get_buffer_ptr(
    sbb_ref     a_buffer
)
{
    return (const void*)a_buffer->buffer;
}

//

bool
sbb_append_buffer(
    sbb_ref     a_buffer,
    const void  *p,
    size_t      p_len
)
{
    bool        is_okay;
    size_t      start_len = a_buffer->length;
    
    if ( (is_okay = __sbb_set_length(a_buffer, a_buffer->length + p_len)) ) {
        memcpy(a_buffer->buffer + start_len, p, p_len);
    }
    return is_okay;
}

//

bool
sbb_append_multiple_buffers(
    sbb_ref     a_buffer,
    const void  *p,
    size_t      p_len,
    ...
)
{
    va_list     vargs;
    bool        is_okay;
    const void  *p2;
    size_t      p2_len, total_len = 0;
    size_t      start_len = a_buffer->length;
    
    if ( (a_buffer->options & sbb_options_immutable) ) return false;
    
    // Calculate total length of addition:
    va_start(vargs, p_len);
    p2 = p, p2_len = p_len;
    while ( p2 ) {
        total_len += p2_len;
        p2 = va_arg(vargs, const void*);
        p2_len = va_arg(vargs, size_t);
    }
    va_end(vargs);
    
    
    // Resize the buffer:
    if ( (is_okay = __sbb_set_length(a_buffer, a_buffer->length + total_len)) ) {
        void    *p1 = a_buffer->buffer + start_len;
    
        va_start(vargs, p_len);
        p2 = p, p2_len = p_len;
        while ( p2 ) {
            memcpy(p1, p2, p2_len);
            p1 += p2_len;
            p2 = va_arg(vargs, const void*);
            p2_len = va_arg(vargs, size_t);
        }
        va_end(vargs);
    }
    return is_okay;
}

//

bool
sbb_append_cstring(
    sbb_ref     a_buffer,
    const char  *cstr
)
{
    return (sbb_append_buffer(a_buffer, cstr, strlen(cstr)) && sbb_append_uint8(a_buffer, 0));
}

//

bool
sbb_append_uint8(
    sbb_ref     a_buffer,
    uint8_t     v
)
{
    bool        is_okay;
    size_t      start_len = a_buffer->length;
    
    // Resize the buffer:
    if ( (is_okay = __sbb_set_length(a_buffer, a_buffer->length + sizeof(uint8_t))) ) {
        *((uint8_t*)(a_buffer->buffer + start_len)) = v;
    }
    return is_okay;
}

//

bool
sbb_append_uint16(
    sbb_ref     a_buffer,
    uint16_t    v
)
{
    bool        is_okay;
    size_t      start_len = a_buffer->length;
    
    // Resize the buffer:
    if ( (is_okay = __sbb_set_length(a_buffer, a_buffer->length + sizeof(uint16_t))) ) {
        if ( (a_buffer->options & sbb_options_byte_swap) ) v = htole16(v);
        *((uint16_t*)(a_buffer->buffer + start_len)) = v;
    }
    return is_okay;
}

//

bool
sbb_append_uint32(
    sbb_ref     a_buffer,
    uint32_t    v
)
{
    bool        is_okay;
    size_t      start_len = a_buffer->length;
    
    // Resize the buffer:
    if ( (is_okay = __sbb_set_length(a_buffer, a_buffer->length + sizeof(uint32_t))) ) {
        if ( (a_buffer->options & sbb_options_byte_swap) ) v = htole32(v);
        *((uint32_t*)(a_buffer->buffer + start_len)) = v;
    }
    return is_okay;
}

//

bool
sbb_append_uint64(
    sbb_ref     a_buffer,
    uint64_t    v
)
{
    bool        is_okay;
    size_t      start_len = a_buffer->length;
    
    // Resize the buffer:
    if ( (is_okay = __sbb_set_length(a_buffer, a_buffer->length + sizeof(uint64_t))) ) {
        if ( (a_buffer->options & sbb_options_byte_swap) ) v = htole64(v);
        *((uint64_t*)(a_buffer->buffer + start_len)) = v;
    }
    return is_okay;
}

//

sbb_deserialize_state_t*
sbb_deserialize_start(
    sbb_ref                 a_buffer,
    sbb_deserialize_state_t *sbb_state
)
{
    if ( ! sbb_state ) sbb_state = malloc(sizeof(sbb_deserialize_state_t));
    if ( sbb_state ) {
        sbb_state->a_buffer = a_buffer;
        sbb_state->idx = 0;
    }
    return sbb_state;
}

//

bool
sbb_deserialize_buffer(
    sbb_deserialize_state_t *sbb_state,
    void                    **p,
    size_t                  *p_len
)
{
    size_t          rem_len = sbb_state->a_buffer->length - sbb_state->idx;
    
    if ( rem_len ) {
        void        *pp;
        size_t      pp_len = *p_len, rem_len = sbb_state->a_buffer->length - sbb_state->idx;
    
        if ( pp_len > rem_len ) pp_len = rem_len;
        if ( pp_len > 0 ) {
            if ( (p == NULL) || (*p == NULL) ) {
                pp = malloc(pp_len);
                if ( ! pp ) return false;
                if ( p ) *p = pp;
            } else {
                pp = (void*)*p;
            }
            memcpy(pp, sbb_state->a_buffer->buffer + sbb_state->idx, pp_len);
            *p_len = pp_len;
            sbb_state->idx += pp_len;
            return true;
        }
    }
    return false;
}

//

bool
sbb_deserialize_cstring(
    sbb_deserialize_state_t *sbb_state,
    char                    **cstr
)
{
    size_t          rem_len = sbb_state->a_buffer->length - sbb_state->idx;
    
    if ( rem_len ) {
        char        *p_start = sbb_state->a_buffer->buffer + sbb_state->idx, *p_end = p_start;
        
        // From current position, walk until finding a NUL character:
        while ( rem_len && *p_end ) rem_len--, p_end++;
    
        // If we didn't find a NUL character, that's an error:
        if ( *p_end ) return false;
        
        // Create the output C string:
        *cstr = malloc(p_end - p_start + 1);
        if ( ! *cstr ) return false;
        memcpy(*cstr, p_start, p_end - p_start + 1);
        
        sbb_state->idx += p_end - p_start + 1;
        return true;
    }
    return false;
}

//

bool
sbb_deserialize_uint8(
    sbb_deserialize_state_t *sbb_state,
    uint8_t                 *v
)
{
    size_t          rem_len = sbb_state->a_buffer->length - sbb_state->idx;
    
    if ( rem_len >= sizeof(uint8_t) ) {
        *v = *((uint8_t*)(sbb_state->a_buffer->buffer + sbb_state->idx));
        sbb_state->idx += sizeof(uint8_t);
        return true;
    }
    return false;
}

//

bool
sbb_deserialize_uint16(
    sbb_deserialize_state_t *sbb_state,
    uint16_t                *v
)
{
    size_t          rem_len = sbb_state->a_buffer->length - sbb_state->idx;
    
    if ( rem_len >= sizeof(uint16_t) ) {
        uint16_t    V = *((uint16_t*)(sbb_state->a_buffer->buffer + sbb_state->idx));
        
        if ( (sbb_state->a_buffer->options & sbb_options_byte_swap) ) V = le16toh(V);
        *v = V;
        sbb_state->idx += sizeof(uint16_t);
        return true;
    }
    return false;
}

//

bool
sbb_deserialize_uint32(
    sbb_deserialize_state_t *sbb_state,
    uint32_t                *v
)
{
    size_t          rem_len = sbb_state->a_buffer->length - sbb_state->idx;
    
    if ( rem_len >= sizeof(uint32_t) ) {
        uint32_t    V = *((uint32_t*)(sbb_state->a_buffer->buffer + sbb_state->idx));
        
        if ( (sbb_state->a_buffer->options & sbb_options_byte_swap) ) V = le32toh(V);
        *v = V;
        sbb_state->idx += sizeof(uint32_t);
        return true;
    }
    return false;
}

//

bool
sbb_deserialize_uint64(
    sbb_deserialize_state_t *sbb_state,
    uint64_t                *v
)
{
    size_t          rem_len = sbb_state->a_buffer->length - sbb_state->idx;
    
    if ( rem_len >= sizeof(uint64_t) ) {
        uint64_t    V = *((uint64_t*)(sbb_state->a_buffer->buffer + sbb_state->idx));
        
        if ( (sbb_state->a_buffer->options & sbb_options_byte_swap) ) V = le64toh(V);
        *v = V;
        sbb_state->idx += sizeof(uint64_t);
        return true;
    }
    return false;
}
