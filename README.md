# PaxoLite
Replicated key value store using paxos for replication and sqlite for storage

## Design
PaxoLite inserts data in a log. LogSeq number is chosen in strictly increasing order. Cluster reaches consensus for each LogSeq using paxos. Data for each LogSeq consists of a key and a value. After a LogSeq entry is learned, all the previous rows for that key are deleted.

## Write Path
Paxos has three phases/roundtrips - Promise, Accept and Learn. PaxoLite uses one more to learn the next LogSeq to be used. Four roundtrips make the write latency very high. Since rows need to be inserted strictly in order, parallel requests don't help. This means, that the writes to PaxoLite are going to be very slow, probably one write per second.

## Read Path
Read is done in two phases. A quorum of nodes is queried to get the best LogSeq number for a learned entry for a key. In the next roundtrip, the value is read from a node. After the first read, this value is cached and requests should get served from memory. Additionaly, the log entries are/can be cached by clients eliminating the need of second round trip on subsequent reads. Second read onwards, therefore, there is only one roundtrip answered from memory. This makes reads from PaxoLite, on average, extreamly fast.
