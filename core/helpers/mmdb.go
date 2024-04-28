package helpers

import (
	"bgptools/utils"
	"log"
	"maps"
	"net"

	"github.com/oschwald/maxminddb-golang"
)

type MMDB struct {
	Cidr          string
	Prefix        int
	PrefixVersion int
	CountryCode   string
	DB            *maxminddb.Reader
}

type DownloadMMDB struct {
	files           []string
	DownloadedFiles map[string]string
	mmdbUrl         string
}

func NewDownloadMMDB(files []string, mmdbUrl string) *DownloadMMDB {
	return &DownloadMMDB{
		files:           files,
		mmdbUrl:         mmdbUrl,
		DownloadedFiles: make(map[string]string),
	}
}

func NewMMDB(cidr string) *MMDB {
	return &MMDB{
		Cidr: cidr,
	}
}

func (mmdb *MMDB) HandleMMDB(path string) error {
	if db, err := maxminddb.Open(path); err != nil {
		return err
	} else {
		mmdb.DB = db
	}
	return nil
}

func (mmdb *MMDB) FindCidr() error {
	ip, network, err := net.ParseCIDR(mmdb.Cidr)
	if err != nil {
		log.Println(err)
	}
	if ip.To4() != nil {
		mmdb.PrefixVersion = 4

	} else {
		mmdb.PrefixVersion = 6
	}
	_ = network
	var record struct {
		Country struct {
			ISOCode string `maxminddb:"iso_code"`
		} `maxminddb:"country"`
	}

	defer mmdb.DB.Close()
	if err := mmdb.DB.Lookup(ip, &record); err != nil {
		return err
	} else {
		mmdb.CountryCode = record.Country.ISOCode
	}
	return nil
}

func (mmdb *DownloadMMDB) GetMMDB() error {
	hDownload := utils.NewDownloadInfo(mmdb.files)
	if err := hDownload.GetFiles(mmdb.mmdbUrl); err != nil {
		return err
	}
	maps.Copy(mmdb.DownloadedFiles, hDownload.Downloads)
	return nil
}
