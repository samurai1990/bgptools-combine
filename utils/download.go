package utils

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type DownloadInfo struct {
	Files     []string
	Downloads map[string]string
}

func NewDownloadInfo(files []string) *DownloadInfo {
	return &DownloadInfo{
		Files:     files,
		Downloads: make(map[string]string),
	}
}

func (u *DownloadInfo) GetFiles(baseUrl string) error {
	wg := sync.WaitGroup{}
	for _, file := range u.Files {
		wg.Add(1)
		go func(f string) {
			newFileName := fmt.Sprintf("%s/%s", TempPath, randomFileName(f))
			url := fmt.Sprintf("%s/%s", baseUrl, f)
			if err := wget(url, newFileName); err != nil {
				log.Printf("Download file %s is failed.", f)
			}
			filenameSplited := strings.Split(f, ".")
			u.Downloads[filenameSplited[0]] = newFileName
			log.Printf("Download file %s is Done.", f)
			defer wg.Done()
		}(file)
	}
	wg.Wait()
	return u.ensureDownloads()
}

func randomFileName(name string) string {
	nameSplite := strings.Split(name, ".")
	date := time.Now().Format("03-04PM--Jan-02-2006")
	fullName := nameSplite[0] + "-" + date + "-" + uuid.NewString() + "." + nameSplite[1]
	return fullName
}

func wget(url, filepath string) error {
	cmd := exec.Command("wget", url, "-O", filepath)
	return cmd.Run()
}

func (u *DownloadInfo) ensureDownloads() error {
	for _, file := range u.Downloads {
		fileInfo, err := os.Stat(file)
		if err != nil || fileInfo.Size() <= 0 {
			return err
		}
	}
	return nil
}
