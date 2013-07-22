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
#include <ngx_md5.h>

#include <math.h>


// http://www.evanmiller.org/nginx-modules-guide.html#directives
static ngx_command_t ngx_http_upstream_consistent_replicated_commands[] = {
    {
        ngx_string("consistent_replicated"),
        NGX_HTTP_UPS_CONF|NGX_CONF_NOARGS|NGX_CONF_TAKE12,
        ngx_http_upstream_consistent_replicated,
        0,
        0,
        NULL
    },

    ngx_null_command
};


// http://www.evanmiller.org/nginx-modules-guide.html#context
static ngx_http_module_t ngx_http_upstream_consistent_replicated_module_ctx = {
    NULL,                               /* preconfiguration */
    NULL,                               /* postconfiguration */

    NULL,                               /* create main configuration */
    NULL,                               /* init main configuration */

    NULL,                               /* create server configuration */
    NULL,                               /* merge server configuration */

    NULL,                               /* create location configuration */
    NULL                                /* merge location configuration */
};


// http://www.evanmiller.org/nginx-modules-guide.html#definition
ngx_module_t ngx_http_upstream_consistent_replicated_module = {
    NGX_MODULE_V1,
    &ngx_http_upstream_consistent_replicated_module_ctx,    /* module context */
    ngx_http_upstream_consistent_replicated_commands,       /* module directives */
    NGX_HTTP_MODULE,                                        /* module type */
    NULL,                                                   /* init master */
    NULL,                                                   /* init module */
    NULL,                                                   /* init process */
    NULL,                                                   /* init thread */
    NULL,                                                   /* exit thread */
    NULL,                                                   /* exit process */
    NULL,                                                   /* exit master */
    NGX_MODULE_V1_PADDING
};


// max possible unsigned int stored in 32bits
// U is not valid in hex numbers and means that number is unsigned
#define CONTINUUM_MAX_POINT  0xffffffffU


typedef struct {
    struct sockaddr                *sockaddr;
    socklen_t                       socklen;
    ngx_str_t                       name;
} ngx_http_upstream_consistent_replicated_peer_addr_t;

typedef struct {
    ngx_uint_t                                              ketama_points;
    ngx_uint_t                                              replication_level;
    ngx_uint_t                                              peers_count;
    ngx_http_upstream_consistent_replicated_peer_addr_t     *peers;
} upstream_consistent_replicated_config_t;


// some prototypes to I can define functions in logical order
static ngx_int_t ngx_http_upstream_init_consistent_replicated (ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us);
static ngx_int_t ngx_http_upstream_init_consistent_replicated (ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *ussv);


/* http://www.evanmiller.org/nginx-modules-guide.html#lb-registration

It registers an upstream initialization function with the surrounding upstream configuration. In addition, the registration function defines which options to the server directive are legal inside this particular upstream block (e.g., weight=, fail_timeout=).
*/
static char * ngx_http_upstream_consistent_replicated (ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
    // directive arguments
    ngx_str_t *value = cf->args->elts;
    // upstream servers config (that variable usually called uscf in another modules)
    ngx_http_upstream_srv_conf_t *ussv;
    // upstream parameters config (this is NOT what is called uscf in some other upstream modules)
    upstream_consistent_replicated_config_t *uscf;

    ussv = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);

    int ketama_points = 150, replication_level = 1;
    unsigned int i;

    // let's parse arguments
    for (i = 1; i < cf->args->nelts; ++i) {
        if (ngx_strncmp(value[i].data, "ketama_points=", 14) == 0) {
            ketama_points = ngx_atoi(&value[i].data[14], value[i].len - 14);

            if (ketama_points == NGX_ERROR || ketama_points <= 0) {
                goto invalid;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "replication_level=", 18) == 0) {
            replication_level = ngx_atoi(&value[i].data[14], value[i].len - 18);

            if (replication_level == NGX_ERROR || replication_level < 0) {
                goto invalid;
            }

            continue;
        }

        goto invalid;
    }

    // in that structure we will store both directive parameters and peers
    uscf = ngx_palloc(cf->pool, sizeof(upstream_consistent_replicated_config));
    if (!uscf) {
        return "not enough memory";
    }

    // fill our config structure with parameters
    uscf->ketama_points     = ketama_points;
    uscf->replication_level = replication_level;

    // fill upstream servers config
    ussv->peer.data = uscf;

    ussv->peer.init_upstream = ngx_http_upstream_init_consistent_replicated;

    ussv->flags = (NGX_HTTP_UPSTREAM_CREATE
                 | NGX_HTTP_UPSTREAM_WEIGHT
                 | NGX_HTTP_UPSTREAM_MAX_FAILS
                 | NGX_HTTP_UPSTREAM_FAIL_TIMEOUT
                 | NGX_HTTP_UPSTREAM_DOWN);

    return NGX_CONF_OK;

invalid:
    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "invalid parameter \"%V\"", &value[i]);

    return NGX_CONF_ERROR;
}


/* http://www.evanmiller.org/nginx-modules-guide.html#lb-upstream

The purpose of the upstream initialization function is to resolve the host names, allocate space for sockets, and assign (yet another) callback.
*/
static ngx_int_t ngx_http_upstream_init_consistent_replicated (ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *ussv) {
    upstream_consistent_replicated_config_t *uscf = ussv->peer.data;
    ngx_http_upstream_server_t *server;
    unsigned int buckets_count, i, j, n;
    ngx_http_upstream_hash_peers_t  *peers;

    /* set the callback */
    ussv->peer.init = ngx_http_upstream_init_consistent_replicated_peer;

    if (!ussv->servers) {
        return NGX_ERROR;
    }

    servers = ussv->servers->elts;

    /* figure out how many IP addresses are in this upstream block. */
    /* remember a domain name can resolve to multiple IP addresses. */
    for (n = 0, i = 0; i < ussv->servers->nelts; i++) {
        n += servers[i].naddrs;
    }

    /* allocate space for sockets, etc */
    peers = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_consistent_replicated_peer_addr_t) * n);

    /* one hostname can have multiple IP addresses in DNS */
    for (n = 0, i = 0; i < ussv->servers->nelts; i++) {
        for (j = 0; j < servers[i].naddrs; j++, n++) {
            peers[n].sockaddr = servers[i].addrs[j].sockaddr;
            peers[n].socklen  = servers[i].addrs[j].socklen;
            peers[n].name     = servers[i].addrs[j].name;
        }
    }

    ussv->peer.data->peers_count = n;
    ussv->peer.data->peers       = peers;

    return NGX_OK;
}


/* http://www.evanmiller.org/nginx-modules-guide.html#lb-peer

The peer initialization function is called once per request. It sets up a data structure that the module will use as it tries to find an appropriate backend server to service that request; this structure is persistent across backend re-tries, so it's a convenient place to keep track of the number of connection failures, or a computed hash value.

In addition, the peer initalization function sets up two callbacks:
  get: the load-balancing function
  free: the peer release function (usually just updates some statistics when a connection finishes)

As if that weren't enough, it also initalizes a variable called tries. As long as tries is positive, nginx will keep retrying this load-balancer.
*/
static ngx_int_t ngx_http_upstream_init_consistent_replicated_peer (ngx_http_request_t *r, ngx_http_upstream_srv_conf_t *ussv) {
    // I would rather call that struct request data, but `peer` seems a convention
    ngx_http_upstream_consistent_replicated_peer_data_t     *ucpd;
    ngx_buf_t *b;

    ngx_http_upstream_consistent_replicated_data_t *ucd = ussv->peer.data;

    ucpd = ngx_pcalloc(r->pool, sizeof(ngx_http_upstream_consistent_replicated_peer_data_t));
    if (ucpd == NULL) {
        return NGX_ERROR;
    }

    r->upstream->peer.data = ucpd;

    r->upstream->peer.free = ngx_http_upstream_free_consistent_replicated_peer;
    r->upstream->peer.get = ngx_http_upstream_get_consistent_replicated_peer;
    // TODO that should be overridable by some variable set per request
    r->upstream->peer.tries = ucd->peer.data->replication_level;

    // TODO get key and length from variable
    ucpd->key.len = b->end - b->start - sizeof("get ") - sizeof(CRLF) + 3;

    ucpd->key.data = ngx_pcalloc(r->pool, ucpd->key.len);

    if (ucpd->key.data == NULL) {
        return NGX_ERROR;
    }

    ngx_cpystrn(ucpd->key.data, b->start+4, ucpd->key.len);

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "upstream_consistent: key \"%V\"", &ucpd->key);

    ucpd->hash = ngx_http_upstream_consistent_ketama_hash(ucpd->key.data, ucpd->key.len-1, 0);

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "upstream_consistent: hash %ui", ucpd->hash);

    ucpd->continuum = ucd->continuum;
    ucpd->continuum_points_counter = ucd->continuum_points_counter;
    ucpd->peers = ucd->peers;

    return NGX_OK;
}


// http://www.evanmiller.org/nginx-modules-guide.html#lb-function
static ngx_int_t ngx_http_upstream_get_consistent_replicated_peer (ngx_peer_connection_t *pc, void *data) {
    ngx_http_upstream_consistent_continuum_item_t *begin, *end, *left, *right, *middle;
    ngx_http_upstream_consistent_peer_data_t  *ucpd = data;
    ngx_http_upstream_consistent_peer_t       *peer;

    pc->cached = 0;
    pc->connection = NULL;

    begin = left = ucpd->continuum;
    end = right = ucpd->continuum + ucpd->continuum_points_counter;

    while (left < right) {
        middle = left + (right - left) / 2;
        if (middle->value < ucpd->hash) {
            left = middle + 1;
        } else {
            right = middle;
        }
    }

    if (right == end) {
        right = begin;
    }
    
    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0, "upstream_consistent: continuum pointer %ui", right->value);

    peer = &ucpd->peers[right->index];

    pc->sockaddr = peer->server->addrs->sockaddr;
    pc->socklen = peer->server->addrs->socklen;
    pc->name = &peer->server->addrs->name;

    return NGX_OK;
}

