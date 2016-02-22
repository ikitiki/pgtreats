/*
 * Copyright (c) 2012, OmniTI Computer Consulting, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 *       copyright notice, this list of conditions and the following
 *       disclaimer in the documentation and/or other materials provided
 *       with the distribution.
 *     * Neither the name OmniTI Computer Consulting, Inc. nor the names
 *       of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written
 *       permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include "postgres.h"
#include "funcapi.h"
#include "utils/tqual.h"
#include "utils/rel.h"
#include "catalog/pg_type.h"
#include "access/htup_details.h"

typedef struct
{
    Relation            rel;
    HeapScanDesc        scan;
    TupleDesc           reltupdesc;
    TupleDesc           tupdesc;
} pg_dirtyread_ctx;

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_dirtyread);
Datum pg_dirtyread(PG_FUNCTION_ARGS);

Datum
pg_dirtyread(PG_FUNCTION_ARGS)
{
    FuncCallContext     *funcctx;
    MemoryContext       oldcontext;
    pg_dirtyread_ctx    *usr_ctx;
    Oid                 relid;
    HeapTuple           tuplein, tupleout;
    TupleDesc           tupdesc;

    if (SRF_IS_FIRSTCALL())
    {
        relid = PG_GETARG_OID(0);

        if (OidIsValid(relid))
        {
            funcctx = SRF_FIRSTCALL_INIT();
            oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
            usr_ctx = (pg_dirtyread_ctx *) palloc(sizeof(pg_dirtyread_ctx));
            usr_ctx->rel = heap_open(relid, AccessShareLock);
            usr_ctx->reltupdesc = RelationGetDescr(usr_ctx->rel);
            get_call_result_type(fcinfo, NULL, &tupdesc);
            funcctx->tuple_desc = BlessTupleDesc(tupdesc);
            usr_ctx->scan = heap_beginscan(usr_ctx->rel, SnapshotAny, 0, NULL);
            usr_ctx->tupdesc = tupdesc;
            funcctx->user_fctx = (void *) usr_ctx;

            if (usr_ctx->tupdesc->natts != usr_ctx->reltupdesc->natts
                && usr_ctx->tupdesc->natts != usr_ctx->reltupdesc->natts + 2)
            {
                elog(
                    ERROR,
                    "Number of returned columns (%d) does not match expected column count (%d or %d).",
                    usr_ctx->tupdesc->natts,
                    usr_ctx->reltupdesc->natts,
                    usr_ctx->reltupdesc->natts + 2
                );
            }

            MemoryContextSwitchTo(oldcontext);
        }
    }

    funcctx = SRF_PERCALL_SETUP();
    usr_ctx = (pg_dirtyread_ctx *) funcctx->user_fctx;

    if ((tuplein = heap_getnext(usr_ctx->scan, ForwardScanDirection)) != NULL)
    {
        int natts = usr_ctx->tupdesc->natts;
        int tnatts = usr_ctx->reltupdesc->natts;

        Datum *odvalues = (Datum *) palloc((tnatts + 1) * sizeof(Datum));
        bool *onulls = (bool *) palloc((tnatts + 1) * sizeof(bool));

        Datum *dvalues = (Datum *) palloc(natts * sizeof(Datum));
        bool *nulls = (bool *) palloc(natts * sizeof(bool));

        heap_deform_tuple(tuplein, usr_ctx->reltupdesc, odvalues + 1, onulls + 1);

        for (int i = 0; i < natts; i++) {
            if (i < tnatts) {
                dvalues[i] = odvalues[i + 1];
                nulls[i] = onulls[i + 1];
            } else if (i == natts - 2) {
                dvalues[i] = UInt32GetDatum(HeapTupleHeaderGetRawXmin(tuplein->t_data));
                nulls[i] = false;
            } else if (i == natts - 1) {
                dvalues[i] = UInt32GetDatum(HeapTupleHeaderGetRawXmax(tuplein->t_data));
                nulls[i] = false;
            }
        }

        tupleout = heap_form_tuple(usr_ctx->tupdesc, dvalues, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tupleout));
    }
    else
    {
        heap_endscan(usr_ctx->scan);
        heap_close(usr_ctx->rel, AccessShareLock);
        SRF_RETURN_DONE(funcctx);
    }
}
