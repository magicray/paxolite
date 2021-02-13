# PaxoLite
Replicated key value store using paxos for replication and sqlite for storage.

## Design
PaxoLite is a CP system, in the context of CAP Theorem. It uses paxos for consensus and hence has no leader election phase. It reads/writes data as soon/long as a quorum of nodes are up.

PaxoLite inserts data in a log. Seq for each log entry is chosen in strictly increasing order. Cluster reaches consensus for each seq using paxos. Data for each seq consists of a key and a value. After a new log entry for a key is learned, all the previous rows for that are deleted.

## Write Path
Paxos has three phases/roundtrips - Promise, Accept and Learn. PaxoLite uses one more to learn the next seq to be used. Four roundtrips make the write latency very high. Since rows need to be inserted strictly in order, parallel requests make it even worse as a currently running paxos round terminates if a new round starts. This makes writes to PaxoLite very slow.

## Read Path
Read is done in two phases. A quorum of nodes is queried to get the best seq number for a learned entry for a key. In the next roundtrip, the value is read from a node. After the first read, this value is cached and requests should get served from memory. In memory caching and no conflicts due to parallel read ensures that the reads from PaxoLite are only limited by hardware constraints and are very fast, on average. As there is no leader, reads are served by all nodes. This ensure strongly consistent reads while still distributing read load on all the nodes.

## Usage
PaxoLite fits well the use cases where number of reads are orders of magnitude more than writes. PaxoLite has no key/value size limitations as data is stored on disk. As recently queried key/value are cached in RAM, it serves these large blobs quite fast. 
