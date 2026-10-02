#!/usr/bin/env sysbench
-- Preserve the pinned Percona workload; only adapt the source cutoff protocol.
local upstream = assert(os.getenv("PRESERVE_TPCC_DIR"), "PRESERVE_TPCC_DIR is required")
package.path = upstream .. "/?.lua;" .. package.path
require("tpcc")

local original_init = thread_init
local original_event = event
local original_restart = sysbench.hooks.before_restart_event
local held = false

local function cutoff(err)
   if type(err) == "table" and err.sql_errno == 4020 then
      if not held then
         held = true
         print(string.format("PRESERVE_4020_HOLD tid=%d", sysbench.tid))
      end
      return true
   end
   return false
end

function thread_init()
   ffi.C.usleep(sysbench.tid * 5000)
   original_init()
end

function sysbench.hooks.before_restart_event(err)
   if cutoff(err) then return end
   -- A normal retry's ROLLBACK can itself meet the source cutoff.
   local ok, restart_error = pcall(original_restart, err)
   if not ok and not cutoff(restart_error) then error(restart_error, 0) end
end

function event()
   if held then
      while true do ffi.C.usleep(1000000) end
   end
   original_event()
end
