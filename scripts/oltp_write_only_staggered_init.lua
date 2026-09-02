#!/usr/bin/env sysbench

-- Keep the stock write-only transaction shape while spreading connection
-- setup across five seconds.  This removes the host listen backlog from the
-- 1000-connection acceptance result.
require("oltp_common")

local ffi = require("ffi")
ffi.cdef[[int usleep(unsigned int usec);]]

local oltp_thread_init = thread_init

function thread_init()
   ffi.C.usleep(sysbench.tid * 5000)
   oltp_thread_init()
end

function prepare_statements()
   if not sysbench.opt.skip_trx then
      prepare_begin()
      prepare_commit()
   end

   prepare_index_updates()
   prepare_non_index_updates()
   prepare_delete_inserts()
end

function event()
   if not sysbench.opt.skip_trx then
      begin()
   end

   execute_index_updates()
   execute_non_index_updates()
   execute_delete_inserts()

   if not sysbench.opt.skip_trx then
      commit()
   end
end
