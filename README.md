HDFS HA for Go
===========

```go
import "github.com/colinmarc/hdfs"
```

```go
ha, _ := hdfs_ha.New("hostname:2181", 5 * time.Second, "nameservice")
server, _ := ha.GetActiveNameNode()
client, _ := hdfs.New(server)
```
