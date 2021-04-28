package utils

import (
	"fmt"
	"github.com/tealeg/xlsx"
)
func ReadExcel(filename string) ([][]string, []string) {

	var listNew [][]string
	xlFile, err := xlsx.OpenFile(filename)

	if err != nil {
		fmt.Printf("open failed: %s\n", err)
	}
	var TbField []string
	for _, sheet := range xlFile.Sheets {

		//fmt.Printf("Sheet Name: %s\n", sheet.Name)
		for k, i := range sheet.Rows[0].Cells{
			if i.Value==""{
				TbField = append(TbField, "Columns"+fmt.Sprint(k))
				continue
			}
			TbField = append(TbField, i.Value)
		}
		// 获取标签页(时间)
		//tmpOra.TIME = sheet.Name
		for k, row := range sheet.Rows {
			if k==0{
				continue
			}
			var rowNew []string
			//fmt.Printf("row: %+v\n", row.Cells)
			////fmt.Println(reflect.ValueOf(row.Cells))
			//for _, i:= range row.Cells{
			//	fmt.Println(reflect.TypeOf(i.Value))
			//	fmt.Printf("Cell: %+v\n", i)
			//}

			for _, cell := range row.Cells {
				//fmt.Printf("cell: %+v\n", *cell)
				if cell.NumFmt=="YYYY-MM-DD HH:MM:SS"{
					t, err := cell.GetTime(false)
					if err!=nil{
						fmt.Println(err)
					}
					//s := t.Format("2006-01-02 15:04:05")
					rowNew = append(rowNew, t.Format("2006-01-02 15:04:05"))
					continue
				}
				text := cell.String()
				rowNew = append(rowNew, text)
			}


			listNew = append(listNew, rowNew)
		}
	}
	return listNew, TbField
}
