package helpers

import (
	"bgptools/utils"
	"maps"
)

type BGP struct {
	baseUrl         string
	DownloadedFiles map[string]string
	Files           []string
}

func NewBGP(url string, files []string) *BGP {
	return &BGP{
		baseUrl:         url,
		Files:           files,
		DownloadedFiles: make(map[string]string),
	}
}

func (b *BGP) GetBgpTools() error {

	hDownload := utils.NewDownloadInfo(b.Files)
	if err := hDownload.GetFiles(b.baseUrl); err != nil {
		return err
	}
	maps.Copy(b.DownloadedFiles, hDownload.Downloads)
	return nil
}
