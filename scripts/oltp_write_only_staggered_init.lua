#!/usr/bin/env sysbench

-- Keep the stock write-only transaction shape while spreading connection
-- setup across five seconds.  This removes the host listen backlog from the
-- 1000-connection acceptance result.
require("oltp_common")

local ffi = require("ffi")
ffi.cdef[[int usleep(unsigned int usec);]]

local oltp_thread_init = thread_init
local oltp_before_restart_event = sysbench.hooks.before_restart_event
local preserve_4020_held = false

function thread_init()
   ffi.C.usleep(sysbench.tid * 5000)
   oltp_thread_init()
end

function sysbench.hooks.before_restart_event(errdesc)
   if errdesc.sql_errno == 4020 then
      if not preserve_4020_held then
         preserve_4020_held = true
         print(string.format("PRESERVE_4020_HOLD tid=%d", sysbench.tid))
      end
      return
   end

   if oltp_before_restart_event ~= nil then
      oltp_before_restart_event(errdesc)
   end
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
   if preserve_4020_held then
      while true do
         ffi.C.usleep(1000000)
      end
   end

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
