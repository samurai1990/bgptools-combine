package helpers

import (
	"bgptools/utils"
	"fmt"
	"log"
	"strings"

	"github.com/spf13/cobra"
)

type cmdConf struct {
	configs *utils.Config
}

type ProcessInterface interface {
	Run()
}

func (a *cmdConf) getCommand() {

	var rootCmd = &cobra.Command{
		Use:   "bgptools",
		Short: "bgptools is a very fast static site generator",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}

	var versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Print the version number of bgptools",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("bgptools Generator v%s", VersionAPP)
		},
	}

	var minioCmd = &cobra.Command{
		Use:   "minio",
		Short: "gather bgptools and send to minio server",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("minio running ...")
			pMinio := ProcessMinioMode{
				cmdConf: &cmdConf{
					configs: a.configs,
				},
			}
			SetCommand(&pMinio)
		},
	}

	var inPut string
	var elasticCmd = &cobra.Command{
		Use:     "elastic",
		Short:   "gather into the elastic server",
		Example: "elastic --input 'minio' or  elastic --input 'web' ",
		Run: func(cmd *cobra.Command, args []string) {
			plec := ProcessElasticMode{
				cmdConf: &cmdConf{
					configs: a.configs,
				},
			}
			switch strings.ToLower(inPut) {
			case "web":
				plec.direct = true
				SetCommand(&plec)
			case "minio":
				SetCommand(&plec)
			default:
				fmt.Println("enter `web` or `minio` as input value")
			}
		},
	}
	elasticCmd.PersistentFlags().StringVarP(&inPut, "input", "i", "", "download source address")
	elasticCmd.MarkPersistentFlagRequired("input")

	rootCmd.AddCommand(versionCmd, minioCmd, elasticCmd)
	if err := rootCmd.Execute(); err != nil {
		log.Fatalln(err)
	}
}

func SetCommand(p ProcessInterface) {
	p.Run()
}

func Execute() {

	conf := utils.NewConfig()
	conf.LoadConfig(".")

	flags := cmdConf{configs: conf}
	flags.getCommand()
}
