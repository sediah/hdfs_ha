package hdfs_ha
import (
	"fmt"
	"strings"
	"time"
	"github.com/golang/protobuf/proto"
	"github.com/samuel/go-zookeeper/zk"
)

const BREADCRUMB_FILENAME = "ActiveBreadCrumb"

type HdfsHa struct {
	ZkConnection     *zk.Conn
	ZkBreadCrumbPath string
}

func New(zkServers string, zkTimeout time.Duration, nameservice string) (*HdfsHa, error) {
	zkConnection, _, err := zk.Connect(strings.Split(zkServers, ","), zkTimeout)
	if err != nil {
		return nil, err
	}
	this := &HdfsHa{}
	this.ZkConnection = zkConnection
	this.ZkBreadCrumbPath = "/hadoop-ha/" + nameservice + "/" + BREADCRUMB_FILENAME
	return this, nil
}

func (this *HdfsHa) GetActiveNameNode() (string, error) {
	data, _, err := this.ZkConnection.Get(this.ZkBreadCrumbPath)
	if err != nil {
		return "", err
	}
	info := &ActiveNodeInfo{}
	if err = proto.Unmarshal(data, info); err != nil {
		return "", err
	}
	return fmt.Sprint(*info.Hostname, ":", *info.Port), nil
}

func (this *HdfsHa) GetActiveNameNodeW() (string, <-chan zk.Event, error) {
	data, _, ech, err := this.ZkConnection.GetW(this.ZkBreadCrumbPath)
	if err != nil {
		return "", nil, err
	}
	info := &ActiveNodeInfo{}
	if err = proto.Unmarshal(data, info); err != nil {
		return "", nil, err
	}
	return fmt.Sprint(*info.Hostname, ":", *info.Port), ech, nil
}

func (this *HdfsHa) Close() {
	this.ZkConnection.Close()
}
