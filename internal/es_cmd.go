/*
Copyright © 2020 SliverHorn

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package internal

import (
	"github.com/gookit/color"
	"github.com/spf13/viper"
	"easy/library"
	"easy/library/utils"
	"github.com/spf13/cobra"
)



// importEsCmd
var importEsCmd = &cobra.Command{
	Use:   "Es",
	Short: "导入数据到ES",
	Long:  `将数据导入到ES`,
	Run: func(cmd *cobra.Command, args []string) {
		method, _ := cmd.Flags().GetString("method")
		path, _ := cmd.Flags().GetString("configpath")
		if path!=""{
			//重新读取配置
			viper.SetConfigFile(path)
			if err := viper.ReadInConfig(); err == nil {
				color.Warn.Println("reset config, file:", viper.ConfigFileUsed())
			}
		}
		switch method {
		case "config":
			library.Es.Initialize()
			err := library.Es.MkConn()
			defer library.Es.Stop()
			if err!=nil{
				panic(err)
			}
			err = library.Es.Ping()
			if err!=nil{
				panic(err)
			}
			err = library.Es.InsertManyDocument(utils.ReadExcel)
			if err!=nil{
				panic(err)
			}
			err = library.Es.Flush()
			if err!=nil{
				panic(err)
			}
			docCount, b, err := library.Es.DocCount()
			if err!=nil{
				panic(err)
			}
			executeResult(docCount, b)
		}
		return
		},
	}



func init() {
	rootCmd.AddCommand(importEsCmd)
	importEsCmd.Flags().StringP("configpath", "c", "", "自定配置文件路径")
	importEsCmd.Flags().StringP("method", "f", "config", "默认config走配置文件形式")
	//importEsCmd.Flags().StringP("doc", "d", "", "指定插入的doc")
	//importEsCmd.Flags().StringP("docId", "i", "", "指定插入doc的id")
}

func executeResult(newC int, b bool) {
	switch b {
	case true:
		color.Info.Printf("插入成功，插入数量与文件一\n")
	case false:
		color.Warn.Printf("插入可能失败，目前插入数量：%d\n", newC)
	}
}