/*!
 * dubug - [du] [b]y [u]ser and [g]roup
 *
 * Fill a static buffer with a human-readable byte count.
 *
 */

#ifndef __BYTE_COUNT_TO_STRING_H__
#define __BYTE_COUNT_TO_STRING_H__

#include "config.h"

const char* byte_count_to_string(double bytes);
const char* byte_count_to_string_buffer(double bytes, char *p, size_t p_len);

#endif /* __BYTE_COUNT_TO_STRING_H__ */
