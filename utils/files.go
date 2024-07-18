package utils

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

type Files struct {
	ListChunkPath []string
}

func NewFiles() *Files {
	return &Files{}
}

func Remove(arg any) {
	switch v := arg.(type) {
	case []string:
		for _, file := range v {
			if err := os.Remove(file); err != nil {
				log.Fatalln(err)
			}
		}
	case map[string]string:
		for _, file := range v {
			if err := os.Remove(file); err != nil {
				log.Fatalln(err)
			}
		}
	}
}

func RemoveTmpDir() error {

	files, err := WalkDir("/tmp", "rosedb-temp*")
	if err != nil {
		return err
	}
	for _, file := range files {
		absolutePath := fmt.Sprintf("/tmp/%s", file)
		if err := os.RemoveAll(absolutePath); err != nil {
			return err
		}
	}
	if err := os.RemoveAll(TempPath); err != nil {
		return err
	}
	return nil
}

func WalkDir(root, exts string) ([]string, error) {
	files, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}

	var matchedFiles []string
	for _, file := range files {
		matched, err := filepath.Match("rosedb-temp*", file.Name())
		if err != nil {
			return nil, err
		}
		if matched {
			matchedFiles = append(matchedFiles, file.Name())
		}
	}
	return matchedFiles, err
}

func EnsureFiles(file string) error {
	_, err := os.Stat(file)
	if os.IsNotExist(err) {
		return fmt.Errorf("file `%s` is not Exist", file)
	}
	return nil
}

func (f *Files) ChunkFile(path string) error {

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	chunkSize := 5000
	chunkIndex := 1
	chunkLines := []string{}

	for scanner.Scan() {
		line := scanner.Text()
		chunkLines = append(chunkLines, line)

		if len(chunkLines) == chunkSize {
			if err := f.saveChunk(chunkLines, chunkIndex); err != nil {
				return err
			}
			chunkIndex++
			chunkLines = []string{}
		}
	}

	if len(chunkLines) > 0 {
		if err := f.saveChunk(chunkLines, chunkIndex); err != nil {
			return err
		}
	}

	return nil
}

func (f *Files) saveChunk(lines []string, index int) error {
	chunkFileName := fmt.Sprintf("%s/chunk_%d.jsonl", BaseChunkPath, index)
	chunkFile, err := os.Create(chunkFileName)
	if err != nil {
		log.Println(err)
		return err
	}
	defer chunkFile.Close()

	writer := bufio.NewWriter(chunkFile)
	for _, line := range lines {
		fmt.Fprintln(writer, line)
	}

	if err := writer.Flush(); err != nil {
		return err
	}
	f.ListChunkPath = append(f.ListChunkPath, chunkFileName)
	return nil
}

func EnsureDir() error {

	if err := RemoveTmpDir(); err == nil {
		if err := os.Mkdir(TempPath, os.FileMode(0766)); err != nil {
			return err
		}
		if err := os.Mkdir(BaseChunkPath, os.FileMode(0766)); err != nil {
			return err
		}

	} else {
		log.Fatalln(err)
	}
	return nil
}

func Initialize() error {
	if err := EnsureDir(); err != nil {
		return err
	}
	return nil
}
