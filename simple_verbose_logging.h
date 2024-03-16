/*!
 * dubug - [du] [b]y [u]ser and [g]roup
 *
 * Logging output helpers.
 *
 */

#ifndef __SIMPLE_VERBOSE_LOGGING_H__
#define __SIMPLE_VERBOSE_LOGGING_H__

#include "config.h"

enum {
    verbosity_critical = -1,
    verbosity_quiet = 0,
    verbosity_error = 1,
    verbosity_warning = 2,
    verbosity_info = 3,
    verbosity_debug = 4
};

int svl_verbosity();
int svl_is_verbosity(int verbosity);
void svl_set_verbosity(int verbosity);
void svl_inc_verbosity();
void svl_dec_verbosity();

void svl_printf(int verbosity, const char *__restrict __format, ...);

#endif /* __SIMPLE_VERBOSE_LOGGING_H__ */
