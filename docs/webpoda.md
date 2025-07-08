# Example PODA queries

All queries have the form below with different URLs
curl --location '<https://lasp.colorado.edu/ops/imap/poda/dap2/packets/SID2/>...' --header 'Authorization: Basic YOUR_USER_PASS_HERE'

Get HK packet count by packet time - finding MAG data
<https://lasp.colorado.edu/ops/imap/poda/dap2/packets/SID2/MAG_HSK_PW.csv?time%3E=2026-01-01T00:00:00&time%3C2030-01-01T00:00:00&count()>

Get time (GSP Microseconds) by human date,and size of HK packets
<https://lasp.colorado.edu/ops/imap/poda/dap2/packets/SID2/MAG_HSK_PW.csv?time%3E=2026-01-01T00:00:00&time%3C2030-01-01T00:00:00&project(time,length)>

Get time/size packets in human readable form
<https://lasp.colorado.edu/ops/imap/poda/dap2/packets/SID2/MAG_HSK_PW.csv?time%3E=2025-04-21T00:00:00&time%3C2025-04-22T00:00:00&&formatTime("yyyy-DDD'T'HH:mm:ss.SSS")&project(time,length)>

Get all packets definition
<https://lasp.colorado.edu/ops/imap/poda/dap2/packets/SID2/>

Get available science and time
<https://lasp.colorado.edu/ops/imap/poda/dap2/packets/SID2/MAG_SCI_NORM.csv?time%3E=2025-04-21T12:34:00&time%3C=2025-04-22T00:00:00&formatTime("yyyy-MM-dd'T'HH:mm:ss.SSS")&project(time,length)>
