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


// some prototypes so I can later use these names before I actually declare them
static char * ngx_http_upstream_consistent_replicated (ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static ngx_int_t ngx_http_upstream_init_consistent_replicated (ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *uscf);
static ngx_int_t ngx_http_upstream_init_consistent_replicated_peer (ngx_http_request_t *r, ngx_http_upstream_srv_conf_t *uscf);
static ngx_int_t ngx_http_upstream_get_consistent_replicated_peer (ngx_peer_connection_t *pc, void *data);
static void ngx_http_upstream_free_consistent_replicated_peer (ngx_peer_connection_t *pc, void *data, ngx_uint_t state);


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

// allowed ketama alg types (perl one uses crc32, libketama - md5)
// perl_cmf stands for "Perl Cache::Memcached::Fast module"
static const char *HASHING_ALG_PERL_CMF  = "perl_cmf";
static const char *HASHING_ALG_LIBKETAMA = "libketama";


typedef struct {
    ngx_http_upstream_server_t                     *server;
    ngx_uint_t                                      addr_index;
    time_t                                          accessed;
    ngx_uint_t                                      fails;
} upstream_consistent_replicated_peer_addr_t;

typedef struct {
    ngx_uint_t                                      point;
    ngx_uint_t                                      index;
} upstream_consistent_replicated_continuum_point_t;

typedef struct {
    upstream_consistent_replicated_continuum_point_t *buckets;
    ngx_uint_t                                      buckets_count;
    ngx_int_t                                       ns_index;
} upstream_consistent_replicated_continuum_t;

// this structure fills up with data during module init
typedef struct {
    ngx_uint_t                                      ketama_points;
    ngx_uint_t                                      replication_level;
    ngx_str_t                                       hashing_alg;
    ngx_uint_t                                      peers_count;
    ngx_uint_t                                      total_weight;
    ngx_int_t                                       repl_var_index;
    ngx_int_t                                       req_key_var_index;
    upstream_consistent_replicated_peer_addr_t     *peers;
    upstream_consistent_replicated_continuum_t     *continuum;
} upstream_consistent_replicated_data_t;

// this structure fills up one time per request and reused while searching for backend
typedef struct {
    ngx_uint_t                                      hash;
    ngx_str_t                                       key;
    ngx_uint_t                                      peer_tries;
    upstream_consistent_replicated_peer_addr_t     *peer;
    upstream_consistent_replicated_data_t          *usd;
} ngx_http_upstream_consistent_peer_data_t;



// variables names
static ngx_str_t replication_level_var  = ngx_string("consistent_replicated_repl_level");
static ngx_str_t requested_key_var      = ngx_string("consistent_replicated_key");


// some service function
static ngx_uint_t consistent_replicated_find_bucket (upstream_consistent_replicated_continuum_t *continuum, unsigned int point) {
    upstream_consistent_replicated_continuum_point_t *left, *right;

    left  = continuum->buckets;
    right = continuum->buckets + continuum->buckets_count;

    while (left < right) {
        upstream_consistent_replicated_continuum_point_t *middle = left + (right - left) / 2;
        if (middle->point < point) {
            left = middle + 1;
        } else if (middle->point > point) {
            right = middle;
        } else {
            /* Find the first point for this value.  */
            while (middle != continuum->buckets && (middle - 1)->point == point) {
                --middle;
            }

            return (middle - continuum->buckets);
        }
    }

    /* Wrap around.  */
    if (left == continuum->buckets + continuum->buckets_count) {
        left = continuum->buckets;
    }

    return (left - continuum->buckets);
}


// some service function
static ngx_uint_t ngx_http_upstream_consistent_replicated_hash(ngx_str_t key, upstream_consistent_replicated_data_t *usd) {
    ngx_uint_t hash;

    if ( ngx_strncmp(usd->hashing_alg.data, HASHING_ALG_PERL_CMF, sizeof(usd->hashing_alg)) == 0 ) {
        hash = ngx_crc32_long(key.data, key.len);

        // don't know what happening here; taken from memcached_hash module.
        if (usd->ketama_points == 0) {
            hash = ((hash >> 16) & 0x00007fffU);
            hash = hash % usd->total_weight;
            hash = (uint64_t) hash * CONTINUUM_MAX_POINT;
            /*
              Shift point one step forward to possibly get from the
              border point which belongs to the previous bucket.
            */
            hash += 1;
        }

    } else {
        // TODO
        hash = 0;

    }

    return hash;
}


// And there goes module functions that Nginx will actually use

/* http://www.evanmiller.org/nginx-modules-guide.html#lb-registration

It registers an upstream initialization function with the surrounding upstream configuration. In addition, the registration function defines which options to the server directive are legal inside this particular upstream block (e.g., weight=, fail_timeout=).
*/
static char * ngx_http_upstream_consistent_replicated (ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
    // directive arguments
    ngx_str_t *value = cf->args->elts;
    // upstream config
    ngx_http_upstream_srv_conf_t *uscf;
    // variable where most of the data stored
    upstream_consistent_replicated_data_t *usd;

    uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);

    int ketama_points = 0, replication_level = 1;
    ngx_str_t hashing_alg = ngx_string(HASHING_ALG_PERL_CMF);
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
            replication_level = ngx_atoi(&value[i].data[18], value[i].len - 18);

            if (replication_level == NGX_ERROR || replication_level < 0) {
                goto invalid;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "hashing_alg=", 12) == 0) {
            ngx_str_t alg = ngx_string(&value[i].data[12]);

            if ( ngx_strncmp(alg.data, HASHING_ALG_PERL_CMF, alg.len + 1) == 0 ) {
                hashing_alg = alg;
            } else if ( ngx_strncmp(alg.data, HASHING_ALG_LIBKETAMA, alg.len + 1) == 0 ) {
                hashing_alg = alg;
            } else {
                goto invalid;
            }

            continue;
        }

        goto invalid;
    }

    // in that structure we will store both directive parameters and peers
    usd = ngx_palloc(cf->pool, sizeof(upstream_consistent_replicated_data_t));
    if (!usd) {
        return "not enough memory";
    }

    // fill our config structure with parameters
    usd->ketama_points     = ketama_points;
ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "ketama %d", ketama_points);
    usd->replication_level = replication_level;
ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "level %d", replication_level);
    usd->hashing_alg       = hashing_alg;
ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "alg %s", hashing_alg.data);

    // fill upstream servers config
    uscf->peer.data = usd;

    uscf->peer.init_upstream = ngx_http_upstream_init_consistent_replicated;

    uscf->flags = (NGX_HTTP_UPSTREAM_CREATE
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
static ngx_int_t ngx_http_upstream_init_consistent_replicated (ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *uscf) {
    upstream_consistent_replicated_data_t      *usd = uscf->peer.data;
    ngx_http_upstream_server_t                 *servers;
    ngx_uint_t                                  i, j;
    upstream_consistent_replicated_peer_addr_t *peers;

    /* set the callback */
    uscf->peer.init = ngx_http_upstream_init_consistent_replicated_peer;

    if (!uscf->servers) {
        return NGX_ERROR;
    }

    servers = uscf->servers->elts;

    /* allocate space for sockets, etc */
    peers = ngx_pcalloc(cf->pool, sizeof(upstream_consistent_replicated_peer_addr_t) * uscf->servers->nelts);
    if (!peers) {
        return NGX_ERROR;
    }

    // fill peers, prepare to create ketama continuum
    usd->total_weight = 0;
    for (i = 0; i < uscf->servers->nelts; i++) {
        ngx_memzero(&peers[i], sizeof(peers[i]));
        peers[i].server = &servers[i];
        usd->total_weight += servers[i].weight;
    }
ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "weight %d", usd->total_weight);

    usd->peers_count = uscf->servers->nelts;
    usd->peers       = peers;

ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "1");
    usd->repl_var_index    = ngx_http_get_variable_index(cf, &replication_level_var);
ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "2");
    usd->req_key_var_index = ngx_http_get_variable_index(cf, &requested_key_var);
ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "3");


    if (usd->ketama_points > 0) {
        // create continuum

        usd->continuum = ngx_pcalloc(cf->pool, sizeof(upstream_consistent_replicated_continuum_t));
        if (!usd->continuum) {
            return NGX_ERROR;
        }

        ngx_uint_t buckets_count = 0;
        for (i = 0; i < uscf->servers->nelts; ++i) {
            buckets_count += usd->ketama_points * servers[i].weight;
        }

        usd->continuum->buckets = ngx_pcalloc(cf->pool, sizeof(upstream_consistent_replicated_continuum_point_t) * buckets_count);
        if (!usd->continuum->buckets) {
            return NGX_ERROR;
        }

        if ( ngx_strncmp(usd->hashing_alg.data, HASHING_ALG_PERL_CMF, sizeof(usd->hashing_alg)) == 0 ) {
            // if using Cache::Memcached::Fast logic (based on crc32 hashing)

            usd->continuum->buckets_count = 0;

            for (i = 0; i < uscf->servers->nelts; ++i) {
                static const char delim = '\0';
                u_char *host, *port;
                size_t len, port_len = 0;
                unsigned int crc32, point, count;

                host = servers[i].addrs[0].name.data;
                len = servers[i].addrs[0].name.len;

#if NGX_HAVE_UNIX_DOMAIN
                if (ngx_strncasecmp(host, (u_char *) "unix:", 5) == 0) {
                    host += 5;
                    len -= 5;
                }
#endif /* NGX_HAVE_UNIX_DOMAIN */

                port = host;
                while (*port) {
                    if (*port++ == ':') {
                        port_len = len - (port - host);
                        len = (port - host) - 1;
                        break;
                    }
                }

                ngx_crc32_init(crc32);
                ngx_crc32_update(&crc32, host, len);
                ngx_crc32_update(&crc32, (u_char *) &delim, 1);
                ngx_crc32_update(&crc32, port, port_len); 

                point = 0;
                count = usd->ketama_points * servers[i].weight;

                for (j = 0; j < count; ++j) {
                    u_char buf[4];
                    unsigned int new_point = crc32;
                    ngx_uint_t bucket;

                    /*
                      We want the same result on all platforms, so we
                      hardcode size of int as 4 8-bit bytes.
                    */
                    buf[0] = point & 0xff;
                    buf[1] = (point >> 8) & 0xff;
                    buf[2] = (point >> 16) & 0xff;
                    buf[3] = (point >> 24) & 0xff;

                    ngx_crc32_update(&new_point, buf, 4);
                    ngx_crc32_final(new_point);
                    point = new_point;

                    if (usd->continuum->buckets_count > 0) {
                        bucket = consistent_replicated_find_bucket(usd->continuum, point);

                        /*
                          Check if we wrapped around but actually have new
                          max point.
                        */
                        if (bucket == 0 && point > usd->continuum->buckets[0].point) {
                            bucket = usd->continuum->buckets_count;

                        } else {
                            /*
                              Even if there's a server for the same point
                              already, we have to add ours, because the
                              first one may be removed later.  But we add
                              ours after the first server for not to change
                              key distribution.
                            */
                            while (bucket != usd->continuum->buckets_count && usd->continuum->buckets[bucket].point == point) {
                                ++bucket;
                            }

                            /* Move the tail one position forward.  */
                            if (bucket != usd->continuum->buckets_count) {
                                ngx_memmove(
                                    usd->continuum->buckets + bucket + 1,
                                    usd->continuum->buckets + bucket,
                                    (usd->continuum->buckets_count - bucket) * sizeof(*usd->continuum->buckets)
                                );
                            }
                        }

                    } else {
                        bucket = 0;
                    }

                    usd->continuum->buckets[bucket].point = point;
                    usd->continuum->buckets[bucket].index = i;

                    ++usd->continuum->buckets_count;

                } // for loop over points per server END

            } // for loop over servers END

        } else {
            // if using libketama algorithm (based on md5 hashing)

            // TODO
            for (i = 0; i < uscf->servers->nelts; i++) {
                float pct = (float) servers[i].weight / (float) usd->total_weight;
                ngx_uint_t points_per_server = floorf( pct * (float) usd->ketama_points / 4 * (float) uscf->servers->nelts );

                for (j = 0; j < points_per_server; j++) {
                    /* 40 hashes, 4 numbers per hash = 160 points per server */
                    char ss[30];
                    unsigned char digest[16];

                    sprintf(ss, "%s-%d", servers[i].addrs[0].name.data, (int) j);

                    ngx_md5_t md5;
                    ngx_md5_init(&md5);
                    ngx_md5_update(&md5, ss, strlen(ss));
                    ngx_md5_final(digest, &md5);

                    /* Use successive 4-bytes from hash as numbers 
                     * for the points on the circle: */
/*                    int h;
                    for (h = 0; h < 4; h++) {
                        usd->continuum[cont].point = ( digest[3+h*4] << 24 )
                                                   | ( digest[2+h*4] << 16 )
                                                   | ( digest[1+h*4] <<  8 )
                                                   |   digest[h*4];

                        memcpy( usd->continuum[cont].ip, slist[i].addr, 22 );
                        cont++;
                    }*/
                }
            }

        }

    } else {
        // if ketama_points == 0

        if ( ngx_strncmp(usd->hashing_alg.data, HASHING_ALG_PERL_CMF, sizeof(usd->hashing_alg)) == 0 ) {
            ngx_uint_t total_weight = 0;

            for (i = 0; i < uscf->servers->nelts; ++i) {
                total_weight += servers[i].weight;

                for (j = 0; j < i; ++j) {
                    usd->continuum->buckets[j].point =
                        (uint64_t) usd->continuum->buckets[j].point
                        * (total_weight - servers[i].weight) / total_weight;
                }

                usd->continuum->buckets[i].point = CONTINUUM_MAX_POINT;
                usd->continuum->buckets[i].index = i;
            }

            usd->continuum->buckets_count = uscf->servers->nelts;

        } else {
            // TODO

        }

    }

    return NGX_OK;
}


/* http://www.evanmiller.org/nginx-modules-guide.html#lb-peer

The peer initialization function is called once per request. It sets up a data structure that the module will use as it tries to find an appropriate backend server to service that request; this structure is persistent across backend re-tries, so it's a convenient place to keep track of the number of connection failures, or a computed hash value.

In addition, the peer initalization function sets up two callbacks:
  get: the load-balancing function
  free: the peer release function (usually just updates some statistics when a connection finishes)

As if that weren't enough, it also initalizes a variable called tries. As long as tries is positive, nginx will keep retrying this load-balancer.
*/
static ngx_int_t ngx_http_upstream_init_consistent_replicated_peer (ngx_http_request_t *r, ngx_http_upstream_srv_conf_t *uscf) {
    // I would rather call that struct request data, but `peer` seems a convention
    ngx_http_upstream_consistent_peer_data_t   *ucpd;
    ngx_http_variable_value_t                  *vv;
    ngx_int_t                                   replication_level;
    ngx_str_t                                   requested_key;

    upstream_consistent_replicated_data_t *usd = uscf->peer.data;

    ucpd = ngx_pcalloc(r->pool, sizeof(ngx_http_upstream_consistent_peer_data_t));
    if (ucpd == NULL) {
        return NGX_ERROR;
    }

    ucpd->usd        = usd;
    ucpd->peer       = NULL;
    ucpd->peer_tries = 0;

    r->upstream->peer.data = ucpd;

    r->upstream->peer.free  = ngx_http_upstream_free_consistent_replicated_peer;
    r->upstream->peer.get   = ngx_http_upstream_get_consistent_replicated_peer;


    /*  get replication level value of request; by default it equals to
        upstream setting which in turn by default equals to one.
    */
    vv = ngx_http_get_indexed_variable(r, usd->repl_var_index);
    if (vv == NULL || vv->not_found || vv->len == 0) {
        replication_level = usd->replication_level;
    } else {
        replication_level = ngx_atoi(vv->data, vv->len);
        if (replication_level == NGX_ERROR || replication_level <= 0) {
            return NGX_ERROR;
        }
    }

    // get requested key
    vv = ngx_http_get_indexed_variable(r, usd->req_key_var_index);
    if (vv == NULL || vv->not_found || vv->len == 0) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "the \"$consistent_replicated_key\" variable is not set");
        return NGX_ERROR;
    } else {
        requested_key.data = vv->data;
        requested_key.len  = vv->len;
    }


    r->upstream->peer.tries = (ngx_uint_t) replication_level;

    ucpd->key = requested_key;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "upstream_consistent: key \"%V\"", &ucpd->key);

    ucpd->hash = ngx_http_upstream_consistent_replicated_hash(ucpd->key, usd);

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "upstream_consistent: hash %ui", ucpd->hash);


    return NGX_OK;
}


/* http://www.evanmiller.org/nginx-modules-guide.html#lb-function

It's time for the main course. The real meat and potatoes. This is where the module picks an upstream.
*/ 
static ngx_int_t ngx_http_upstream_get_consistent_replicated_peer (ngx_peer_connection_t *pc, void *data) {
    ngx_http_upstream_consistent_peer_data_t   *ucpd = data;
    upstream_consistent_replicated_data_t      *usd  = ucpd->usd;
    upstream_consistent_replicated_peer_addr_t *peer = ucpd->peer;
    ngx_addr_t                                 *addr;

    /*
      I don't really understand why we do this, but maybe the reason is that connection caching is now
      done via upstream_keepalive module.
    */
    pc->cached = 0;
    pc->connection = NULL;

    if (!peer) {
        ngx_uint_t bucket = consistent_replicated_find_bucket(usd->continuum, ucpd->hash);
        peer = &usd->peers[ usd->continuum->buckets[bucket].index ];
        peer->addr_index = 0;

        ucpd->peer_tries = peer->server->naddrs;
    }

    if (peer->server->down) {
        goto fail;
    }

    if (peer->server->max_fails > 0 && peer->fails >= peer->server->max_fails) {
        time_t now = ngx_time();
        if (now - peer->accessed <= peer->server->fail_timeout) {
            goto fail;
        } else {
            peer->fails = 0;
        }
    }

    addr = &peer->server->addrs[peer->addr_index];

    pc->sockaddr =  addr->sockaddr;
    pc->socklen  =  addr->socklen;
    pc->name     = &addr->name;

    return NGX_OK;

fail:
    return NGX_BUSY;
}


/* http://www.evanmiller.org/nginx-modules-guide.html#lb-release

The peer release function operates after an upstream connection takes place; its purpose is to track failures.
*/
static void ngx_http_upstream_free_consistent_replicated_peer (ngx_peer_connection_t *pc, void *data, ngx_uint_t state) {
    ngx_http_upstream_consistent_peer_data_t   *ucpd = data;
    upstream_consistent_replicated_peer_addr_t *peer = ucpd->peer;

    if (state & NGX_PEER_FAILED) {
        if (peer->server->max_fails > 0) {
            time_t now = ngx_time();
            if (now - peer->accessed > peer->server->fail_timeout) {
                peer->fails = 0;
            }

            ++peer->fails;

            if (peer->fails == 1 || peer->fails == peer->server->max_fails) {
                peer->accessed = ngx_time();
            }
        }

        if (--ucpd->peer_tries > 0) {
            // first we should try all addresses of this peer...
            if (++peer->addr_index == peer->server->naddrs) {
                peer->addr_index = 0;
            }

        } else {
            // ... then move to the next peer (in case we have replication_level > 1)
            --pc->tries;

        }

    } else if (state & NGX_PEER_NEXT) {
        /*
          If memcached gave negative (NOT_FOUND) reply, there's no need
          to try the same cache though different address.
        */
        pc->tries = 0;

    }
}

