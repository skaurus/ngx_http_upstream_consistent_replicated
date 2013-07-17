/*
 * Map a variable to one or few backend servers.
 *
 * Copyright (C) Dmitry Shalashov
 *
 * This module can be distributed under the same terms as Nginx itself.
 */

// https://github.com/dctrwatson/nginx-upstream-consistent/blob/master/ngx_http_upstream_consistent_module.c
// http://www.evanmiller.org/nginx-modules-guide.html
// http://www.evanmiller.org/nginx/ngx_http_upstream_hash_module.c.txt
// http://openhack.ru/nginx-patched/wiki/MemcachedHash

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>


