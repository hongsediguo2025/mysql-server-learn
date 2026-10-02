#!/usr/bin/env sysbench

-- Use the stock read/write transaction and PS definitions. Keep the same
-- staggered connection setup and 4020 hold contract as the write-only run.
require("oltp_read_write")

local ffi = require("ffi")
ffi.cdef[[int usleep(unsigned int usec);]]

local oltp_thread_init = thread_init
local oltp_event = event
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

function event()
   if preserve_4020_held then
      while true do
         ffi.C.usleep(1000000)
      end
   end
   oltp_event()
end
