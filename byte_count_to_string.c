/*!
 * dubug - [du] [b]y [u]ser and [g]roup
 *
 * Fill a static buffer with a human-readable byte count.
 *
 */

#include "byte_count_to_string.h"

const char*
byte_count_to_string(
    double      bytes
)
{
    static char outbuffer[64];
    return byte_count_to_string_buffer(bytes, outbuffer, sizeof(outbuffer));
}

//

const char*
byte_count_to_string_buffer(
    double      bytes,
    char        *p,
    size_t      p_len
)
{
    static const char* units[] = { "  B", "KiB", "MiB", "GiB", "TiB", "PiB", NULL };

    int         unit = 0, out_len;

    while ( bytes > 1024.0 ) {
        if ( ! units[unit + 1] ) break;
        bytes /= 1024.0;
        unit++;
    }
    out_len = snprintf(p, (p ? p_len : 0), "%.2lf %s", bytes, units[unit]);
    if ( ! p ) {
        p = malloc((p_len = out_len + 1));
        if ( p ) {
            out_len = snprintf(p, (p ? p_len : 0), "%.2lf %s", bytes, units[unit]);
        }
    }
    if ( p && (out_len >= p_len) ) p = NULL;
    return p;
}
