package hdfs_ha

import (
	"fmt"
	"testing"
	"time"
	"github.com/colinmarc/hdfs"
	"os"
)

func TestGetActiveNameNode(t *testing.T) {
	zkServers := os.Getenv("HA_ZKSERVERS")
	nameservice := os.Getenv("HA_NAMESPACE")
	if zkServers == "" {
		zkServers = "tokyo063.dakao.io:2181,tokyo064.dakao.io:2181,tokyo062.dakao.io:2181"
	}
	if nameservice == "" {
		nameservice = "aa"
	}
	ha, err := New(zkServers, 5 * time.Second, nameservice, true)
	if err != nil {
		t.Error(err)
	}
	server, err := ha.GetActiveNameNode()
	if err != nil {
		t.Error(err)
	}
	client, err := hdfs.New(server)
	if err != nil {
		t.Error(err)
	}
	files, err := client.ReadDir("/")
	if err != nil {
		t.Error(err)
	}
	for _, file := range files {
		fmt.Println("/" + file.Name())
	}
	ha.Close()
}
