
#pragma once

#include "ai_btree.h"
#include "base/udf_record.h"
#include "base/udf_memtracker.h"

#include <aerospike/as_list.h>
#include <aerospike/as_stream.h>
#include <aerospike/as_rec.h>
#include <aerospike/as_val.h>
#include <aerospike/mod_lua.h>
#include <aerospike/as_buffer.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_string.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_map.h>
#include <aerospike/as_list.h>

#include <citrusleaf/cf_ll.h>

typedef enum as_aggr_caller_type
{
	AS_AGGR_SCAN,
	AS_AGGR_QUERY,
} as_aggr_caller_type;



typedef struct as_aggr_caller_intf_s {

void (*set_error)(void * caller);
bool (*mem_op)(mem_tracker *mt, uint32_t num_bytes, memtracker_op op);
as_aggr_caller_type (*get_type)( );

}as_aggr_caller_intf;

typedef struct query_record_s {
	as_rec               * urec;
	void                 * caller;
	udf_record           * urecord;
	bool                   read;
} query_record;

typedef struct as_aggr_istream_s {
	cf_ll_iterator  *iter;
	as_rec          *rec;
	dig_arr_t       *dt;
	int              dtoffset;
	as_namespace    *ns;
	as_aggr_caller_type (*get_type)( );
} as_aggr_istream;

typedef enum as_query_udf_op
{
	AS_QUERY_UDF_OP_UDF,
	AS_QUERY_UDF_OP_AGGREGATE,
	AS_QUERY_UDF_OP_MR
} as_query_udf_op;

typedef struct as_aggr_call_s {
	bool                   active;
	as_namespace           * ns;
	char                   filename[UDF_MAX_STRING_SZ];
	char                   function[UDF_MAX_STRING_SZ];
	as_msg_field           * arglist;
	void                   * caller;
	as_aggr_caller_intf    * caller_intf;
	as_stream_hooks        * istream_hooks;
	as_stream_hooks        * ostream_hooks;
} as_aggr_call;

as_val * as_aggr_istream_read(const as_stream *s); 
/*
typedef struct as_aggr_call_s {
	bool                   active;
    as_namespace           * ns;
	char                   filename[UDF_MAX_STRING_SZ];
	char                   function[UDF_MAX_STRING_SZ];
	as_msg_field           * arglist;
    void                   * caller;
    as_aggr_caller_intf    * caller_intf;
    as_stream_hooks        * istream_hooks;
    as_stream_hooks        * ostream_hooks;
} as_aggr_call;
*/
