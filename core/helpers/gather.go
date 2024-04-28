package helpers

import (
	"bgptools/utils"
	"log"
	"maps"
)

type CollentInfo struct {
	Files           []string
	bpgurl          string
	mmdbUrl         string
	DownloadedFiles map[string]string
}

type CollentInfoMinio struct {
	*Storage
	DownloadedFiles map[string]string
}

func NewCollentInfo(bgpToolsUrl, MMDBToolsUrl string) *CollentInfo {
	return &CollentInfo{
		bpgurl:          bgpToolsUrl,
		mmdbUrl:         MMDBToolsUrl,
		DownloadedFiles: make(map[string]string),
	}
}

func NewCollentMinioInfo(object *Storage) *CollentInfoMinio {
	return &CollentInfoMinio{
		Storage:         object,
		DownloadedFiles: make(map[string]string),
	}
}

func (c *CollentInfo) GatherFiles() error {

	bgpF := []string{"asns.csv", "table.jsonl"}
	bgp := NewBGP(c.bpgurl, bgpF)

	if err := bgp.GetBgpTools(); err != nil {
		return err
	}
	maps.Copy(c.DownloadedFiles, bgp.DownloadedFiles)

	Hmmdb := NewDownloadMMDB([]string{"GeoLite2-Country.mmdb"}, c.mmdbUrl)
	if err := Hmmdb.GetMMDB(); err != nil {
		return err
	}
	maps.Copy(c.DownloadedFiles, Hmmdb.DownloadedFiles)
	return nil
}

func (c *CollentInfoMinio) GatherMinio() error {

	typeFiles := []string{"table", "asns", "GeoLite2-Country"}
	c.Storage.ListTypeFile = typeFiles
	errS3, objcsS3 := c.Storage.ListObjectS3()
	if errS3 != nil {
		log.Fatal(errS3)
	}


	if err := c.Storage.LastFileS3(objcsS3); err != nil {
		log.Fatal(err)
	}
	if err := c.Storage.GetS3(); err != nil {
		log.Fatal(err)
	}


	tarGZ := utils.NewTAR()
	tarGZ.TarFiles = c.Storage.EnsureFiles
	if err := tarGZ.ExtractTarGz(); err != nil {
		log.Fatalln(err)
	}
	maps.Copy(c.DownloadedFiles, tarGZ.ExtraxtFiles)
	return nil
}
