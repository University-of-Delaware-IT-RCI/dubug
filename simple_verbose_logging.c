/*!
 * dubug - [du] [b]y [u]ser and [g]roup
 *
 * Logging output helpers.
 *
 */

#include "simple_verbose_logging.h"
#include <stdarg.h>

static const char* svl_verbosity_string[] = {
            "QUIET",
            "ERROR",
            "WARNING",
            "INFO",
            "DEBUG"
        };

//

static int svl_verbosity_level = verbosity_error;
static bool svl_should_show_timestamps = false;

//

int
svl_verbosity()
{
    return svl_verbosity_level;
}

//

int
svl_is_verbosity(
    int     verbosity
)
{
    return ( verbosity <= svl_verbosity_level );
}

//

void
svl_set_verbosity(
    int     verbosity
)
{
    if ( verbosity < verbosity_quiet )
        verbosity = verbosity_quiet;
    else if ( verbosity > verbosity_debug )
        verbosity = verbosity_debug;
    svl_verbosity_level = verbosity;
}

//

void
svl_inc_verbosity()
{
    svl_set_verbosity(svl_verbosity_level + 1);
}

//

void
svl_dec_verbosity()
{
    svl_set_verbosity(svl_verbosity_level - 1);
}

//

bool
svl_get_show_timestamps()
{
    return svl_should_show_timestamps;
}

//

void
svl_set_show_timestamps(
    bool    should_show_timestamps
)
{
    svl_should_show_timestamps = should_show_timestamps;
}

//

#ifdef HAVE_MPI

# include "mpi.h"

static int mpi_rank = 0;
static int mpi_size = 1;

void
svl_mpi_init(void)
{
    static int svl_mpi_is_inited = 0;
    
    if ( ! svl_mpi_is_inited ) {
        MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
        MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
        svl_mpi_is_inited = 1;
    }
}

#endif

//

void
svl_printf(
    int         verbosity,
    const char  *__restrict __format,
    ...
)
{
    if ( (verbosity == verbosity_critical) || (verbosity <= svl_verbosity_level) ) {
        va_list     vargs;
        int         format_len = strlen(__format);
        
        if ( svl_should_show_timestamps ) {
            char        timestamp_str[24];
            time_t      now;
            struct tm   *now_tm;
            size_t      n_char;

            now = time(NULL);
            now_tm = localtime(&now);
            n_char = strftime(timestamp_str, sizeof(timestamp_str), "%Y%d%m %H:%M:%S%z", now_tm);
            fprintf(stderr, "[%s]", timestamp_str);
        }
        
        va_start(vargs, __format);
#ifdef HAVE_MPI
        svl_mpi_init();
        fprintf(stderr, "[%s][MPI::%d/%d] ", svl_verbosity_string[verbosity], mpi_rank, mpi_size);
#else
        fprintf(stderr, "[%s] ", svl_verbosity_string[verbosity]);
#endif
        if ( format_len > 0 ) {
            vfprintf(stderr, __format, vargs);
            if ( __format[format_len - 1] != '\n' ) fputc('\n', stderr);
        } else {
            fputc('\n', stderr);
        }
        va_end(vargs);
    }
}
