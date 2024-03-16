
#include "sbb.h"

int
main()
{
    sbb_ref     S = sbb_alloc(0, sbb_options_byte_swap);
    
    sbb_append_uint8(S, 0xAD);
    sbb_append_uint16(S, 0xC0DE);
    sbb_append_uint32(S, 0xFEEDFACE);
    sbb_append_cstring(S, "This is our test!");
    sbb_summary(S); printf("\n");
    
    sbb_ref     Sprime = sbb_alloc_with_const_buffer(0, sbb_get_buffer_ptr(S), sbb_get_length(S));
    
    sbb_append_uint16(S, 0xC0DE);
    sbb_summary(Sprime); printf("\n");
    
    uint8_t     i8;
    uint16_t    i16;
    uint32_t    i32;
    char        *cstr;
    
    SBB_DESERIALIZE_BEGIN(Sprime)
        SBB_DESERIALIZE_UINT8(Sprime, i8)
        SBB_DESERIALIZE_UINT16(Sprime, i16)
        SBB_DESERIALIZE_UINT32(Sprime, i32)
        SBB_DESERIALIZE_CSTRING(Sprime, cstr)
    SBB_DESERIALIZE_ON_ERROR(Sprime)
        printf("Failure to decode buffer.\n");
    SBB_DESERIALIZE_OTHERWISE(Sprime)
        printf("%02hhX %04hX %08X %s\n", i8, i16, i32, cstr);
    SBB_DESERIALIZE_END(Sprime)
    
    sbb_free(Sprime);
    sbb_free(S);
    return 0;
}
