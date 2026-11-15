@echo off
rem zhdata client: 32 slots (49888-49919). 64 slots tested: home line dropped 24/64 conns during speedtest bursts - staying on 32.
rem v0.11 (route A): uploads use a byte window released only by server DELIVERED acks + a retransmit buffer
rem (stalled/killed lanes are re-sent over the other lanes), so per-flow spreading is safe again:
rem -spread defaults to 8 (auto-budget: min(spread, slots/live bulk streams), global lane cap = slots).
rem Needs the matching server build (handshake magic zhv4); client+server must be upgraded together.
"%~dp0zhdata-tunnel-windows-amd64.exe" -mode client -listen 127.0.0.1:49888 -server C498.uwintech.com:49888,C498.uwintech.com:49889,C498.uwintech.com:49890,C498.uwintech.com:49891,C498.uwintech.com:49892,C498.uwintech.com:49893,C498.uwintech.com:49894,C498.uwintech.com:49895,C498.uwintech.com:49896,C498.uwintech.com:49897,C498.uwintech.com:49898,C498.uwintech.com:49899,C498.uwintech.com:49900,C498.uwintech.com:49901,C498.uwintech.com:49902,C498.uwintech.com:49903,C498.uwintech.com:49904,C498.uwintech.com:49905,C498.uwintech.com:49906,C498.uwintech.com:49907,C498.uwintech.com:49908,C498.uwintech.com:49909,C498.uwintech.com:49910,C498.uwintech.com:49911,C498.uwintech.com:49912,C498.uwintech.com:49913,C498.uwintech.com:49914,C498.uwintech.com:49915,C498.uwintech.com:49916,C498.uwintech.com:49917,C498.uwintech.com:49918,C498.uwintech.com:49919 -uuid 4ae0ae4d-cbf9-44e9-8513-a9c797eedd93 -conns 32 -v
pause

