/*
Package tasker is a light distribute producer&consumer task model based on beego.

Task

the main description of message or job. the state machine like this:

                                   +----> failed
                                   |
   pending ----+----> running -----+----> success
               ^                   |
               |                   v
               +---------------- retry

*/
package tasker

import (
	"errors"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"github.com/NoneBorder/dora"
	"github.com/astaxie/beego/orm"
	"github.com/sony/sonyflake"
)

// Core is the task package config table.
//
// `MasterOutOfDate` means the time master state can be save, instance can race to be master when the duration of `Updated` to now bigger than this.
// `InstanceHeartbeat` is the max interval Instance should be check Master, should be less than `MasterOutOfDate`
type Core struct {
	Id                uint64
	MasterInstanceID  uint16    `orm:"column(master_instance_id)"`
	MasterFQDN        string    `orm:"column(master_fqdn)"`
	Updated           time.Time `orm:"auto_now"`
	MasterOutOfDate   int64 // ms
	InstanceHeartbeat int64 // ms
}

const (
	// defaultMasterOutOfDate is the default MasterOutOfDate value
	defaultMasterOutOfDate = 6000
	// defaultInstanceHeartbeat is the default InstanceHeartbeat value
	defaultInstanceHeartbeat = 3000
)

// UniqID use for generate worker id.
var UniqID *sonyflake.Sonyflake

// InstanceID is the tasker instance uniq key.
var InstanceID uint16

// IsMaster when true, the instance is master instance.
var IsMaster bool = false

/*
Init will initialize the tasker instance, include:

	1. generate the InstanceID use MachineID func, use instance private ip address when MachineID is nil
	2. start race master in goroutine
	3. initialize all task
*/
func Init(MachineID func() (uint16, error), CheckMachineID func(uint16) bool) (err error) {
	if MachineID == nil {
		MachineID = lower16BitPrivateIP
	}

	if InstanceID, err = MachineID(); err != nil {
		return
	}

	fqdn = getFQDN()
	dora.Info().Msgf("tasker started with InstanceID %d FQDN %s", InstanceID, FQDN())

	if CheckMachineID != nil {
		if !CheckMachineID(InstanceID) {
			return errors.New("not valid MachineID via CheckMachineID method validate")
		}
	}

	if err = InitIDGEN(MachineID, CheckMachineID); err != nil {
		// InitIDGEN failed
		return
	}

	go keepFQDNUpdating()

	justWorker := strings.ToLower(os.Getenv("NB_TASKER_JUST_WORKER"))
	if justWorker != "on" && justWorker != "true" {
		go keepMasterRace()
	}

	InitAllTask()

	return nil
}

// InitIDGEN is initialize the ID generator, does not need invoke if had use `Init`
func InitIDGEN(MachineID func() (uint16, error), CheckMachineID func(uint16) bool) error {
	// init sonyflake.
	UniqID = sonyflake.NewSonyflake(sonyflake.Settings{
		StartTime:      time.Now(),
		MachineID:      MachineID,
		CheckMachineID: CheckMachineID,
	})
	if UniqID == nil {
		return errors.New("initialize unique id generate tool failed")
	}
	return nil
}

func RegisterModel() {
	orm.RegisterModel(new(Core), new(Task))
}

func getCore() (core *Core, err error) {
	o := orm.NewOrm()
	core = &Core{Id: 1} // alwary read id=1
	err = o.Read(core)
	return
}

func (self *Core) becomeMaster() {
	self.MasterInstanceID = InstanceID
	self.MasterFQDN = FQDN()

	o := orm.NewOrm()
	var err error
	if self.Id != 0 {
		// save
		_, err = o.Update(self, "MasterInstanceID", "MasterFQDN", "Updated")
	} else {
		// insert
		_, err = o.Insert(self)
	}

	if err != nil {
		dora.Error().Msgf("[tasker] try to becomeMaster failed: %s", err.Error())
	}
}

func (self *Core) heartbeatMaster() {
	o := orm.NewOrm()
	_, err := o.Update(self, "Updated")
	if err != nil {
		dora.Error().Msgf("[tasker] heartbeatMaster failed: %s", err.Error())
	}
}

// keepMasterRace always race to be master
func keepMasterRace() {
	rand.Seed(time.Now().Unix())

	for {
		core, err := getCore()
		if err != nil {
			if err == orm.ErrNoRows {
				// no core object, register self
				core.Id = 0
				core.MasterOutOfDate = defaultMasterOutOfDate
				core.InstanceHeartbeat = defaultInstanceHeartbeat
				core.becomeMaster()
				continue
			}

			// output the error log and sleep 1s
			dora.Error().Msgf("[tasker] get core config from db failed: %s", err.Error())
			time.Sleep(1 * time.Second)
			continue
		}

		if core.MasterInstanceID == InstanceID {
			// I'm master
			IsMaster = true
			core.heartbeatMaster()
		} else {
			// I'm not master
			IsMaster = false
			if time.Since(core.Updated).Nanoseconds()/int64(time.Millisecond) > core.MasterOutOfDate {
				// the master outofdate, I will be the master
				core.becomeMaster()
			}
		}

		time.Sleep(time.Duration(core.InstanceHeartbeat-rand.Int63n(2000)) * time.Millisecond)
	}
}

func lower16BitPrivateIP() (uint16, error) {
	ip, err := privateIPv4()
	if err != nil {
		return 0, err
	}

	return uint16(ip[2])<<8 + uint16(ip[3]), nil
}

func privateIPv4() (net.IP, error) {
	as, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	for _, a := range as {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}

		ip := ipnet.IP.To4()
		if ip != nil {
			return ip, nil
		}
	}
	return nil, errors.New("no private ip address")
}

var fqdn string

// FQDN Get Fully Qualified Domain Name
// returns "unknown" or hostanme in case of error
func FQDN() string {
	return fqdn
}

func keepFQDNUpdating() {
	for {
		fqdn = getFQDN()
		time.Sleep(180 * time.Second)
	}
}

func getFQDN() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}

	addrs, err := net.LookupIP(fqdn)
	if err != nil {
		return hostname
	}

	for _, addr := range addrs {
		if ipv4 := addr.To4(); ipv4 != nil {
			ip, err := ipv4.MarshalText()
			if err != nil {
				return hostname
			}
			hosts, err := net.LookupAddr(string(ip))
			if err != nil {
				return hostname
			}
			hostname = strings.TrimSuffix(hosts[0], ".") // return fqdn without trailing dot
			return hostname
		}
	}

	return hostname
}
