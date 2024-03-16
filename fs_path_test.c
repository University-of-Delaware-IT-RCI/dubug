
#include "fs_path.h"

int
main()
{
    fs_path_ref     p = fs_path_alloc_with_cstring(0, "/x/y/z");
    char            b[16];
    const char      *b_ptr, *b_end;
    
    printf("Allocate from cstring:\n");
    printf("    %p -> \"%s\" %d %d\n", p, fs_path_get_cstring(p), fs_path_get_length(p), fs_path_get_capacity(p));
    
    fs_path_ref     relp = fs_path_alloc_with_cstring_relative_to(p, "a/b/c.txt");
    
    printf("Popping path components:\n");
    while ( fs_path_pop(p) ) {
        printf("    %p -> \"%s\" %d %d\n", p, fs_path_get_cstring(p), fs_path_get_length(p), fs_path_get_capacity(p));
    }
    
    printf("Allocate relative to base path:\n");
    printf("    %p -> \"%s\" %d %d\n", relp, fs_path_get_cstring(relp), fs_path_get_length(relp), fs_path_get_capacity(relp));
    
    fs_path_free(p);
    
    p = fs_path_alloc_with_path(relp);
    fs_path_free(relp);
    
    printf("Allocate with existing path obj:\n");
    printf("    %p -> \"%s\" %d %d\n", p, fs_path_get_cstring(p), fs_path_get_length(p), fs_path_get_capacity(p));
    fs_path_free(p);
    
    p = fs_path_alloc_with_cstring(0, "this/is/a/relative/path");
    
    printf("Allocate from cstring (relative path):\n");
    printf("    %p -> \"%s\" %d %d\n", p, fs_path_get_cstring(p), fs_path_get_length(p), fs_path_get_capacity(p));
    
    printf("Copy string to external buffer:\n");
    b_end = fs_path_copy(p, b, sizeof(b));
    printf("    %p -> %p -> \"", p, b);
    b_ptr = b;
    while ( b_ptr < b_end ) fputc(*b_ptr++, stdout);
    printf("\" %d\n", b_end - b);
    
    printf("Pop path components from relative path:\n");
    printf("    %p -> \"%s\" %d %d\n", p, fs_path_get_cstring(p), fs_path_get_length(p), fs_path_get_capacity(p));
    while ( fs_path_pop(p) ) {
        printf("    %p -> \"%s\" %d %d\n", p, fs_path_get_cstring(p), fs_path_get_length(p), fs_path_get_capacity(p));
    }
    
    fs_path_push_characters(p, "/a/b/c/d...........", 8);
    printf("Push characters to existing path:\n");
    printf("    %p -> \"%s\" %d %d\n", p, fs_path_get_cstring(p), fs_path_get_length(p), fs_path_get_capacity(p));
    printf("Popping path components:\n");
    while ( fs_path_pop(p) ) {
        printf("    %p -> \"%s\" %d %d\n", p, fs_path_get_cstring(p), fs_path_get_length(p), fs_path_get_capacity(p));
    }
    fs_path_free(p);
    
    return 0;
}
