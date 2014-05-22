This tool detects latency anomaly, find spike events and non spike events timestamp.
Creates a directory textfiles under current directory and stores events and no events files for each router.

For example:

Router R1 saw spikes at t2 to s1, s4, s5 servers and no spikes at t2 to s2, s3,s6 and servers s7,s8,s9 didn't have measurement for 
t2 timestamp.

Then files, for example R1_dict.txt, under ./textfiles/ folder will have { t2:[s1,s4,s5] } and so on for other timestamps
files under noevent folder will have { t2:[s2,s3,s6] } and so on for other timestamps.

main.py - is the tool which does the tasks described above. It requires pgsql.py. It needs to be run on the server where bismark active
database is stored (dp4).

cp_paris_main.py - finds AS path to all the servers and using the spike event and non spike event files for each router finds the 
hitting set and keeps a counter for peering link appearing in the hitting set and also AS appearing in the hitting set. It requires 
paristraceroute.py for fetching data from the database "swati" on dp5.
