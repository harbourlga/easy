package library

import (
	"context"
	"easy/library/utils"
	"fmt"
	"testing"
)

var Index = "test"
var Url = "http://81.68.145.26:9200"

func Test_es_InsertOneDocument(t *testing.T) {
	Es.cfg.Index = Index
	Es.cfg.URL = Url
	err := Es.MkConn()
	if err!=nil{
		panic(err)
	}
	defer Es.Stop()
	err = Es.InsertOneDocument("6", `{"content": "狼性思维的核心之一是，解决不了问题，就先解决提出问题的人。"}`)
	if err!=nil{
		panic(err)
	}
	result, err := Es.client.Search().Index(Index).Do(context.Background())
	if err!=nil{
		panic(err)
	}
	fmt.Println(result)
	r, err := Es.client.Delete().Index(Index).Id("6").Do(context.Background())
	if err!=nil{
		panic(err)
	}
	fmt.Println(r)
}

func Test_es_InsertManyDocument(t *testing.T) {
	Es.cfg.Index = Index
	Es.cfg.URL = Url
	Es.insertFile = "./SmoothNLP专栏资讯数据集样本10k.xlsx"
	err := Es.MkConn()
	if err!=nil{
		panic(err)
	}
	defer Es.Stop()
	err = Es.InsertManyDocument(utils.ReadExcel)
	if err!=nil{
		panic(err)
	}

	_, err = Es.client.Flush(Es.cfg.Index).Do(context.Background()) //刷脏数据
	if err!=nil{
		panic(err)
	}
	catCount, err := Es.client.CatCount().Index(Es.cfg.Index).Do(context.Background())  //计算目标INDEX的DOC数量
	if err!=nil{
		panic(err)
	}
	//c, err := Es.client.CatIndices().Do(context.Background())
	//fmt.Printf("%+v\n", c[1])

	for _, c := range catCount{
		if c.Count!=Es.count{
			t.Fatal("插入数量与目标不一致")
		}
	}

	err = Es.DeleteIndex() //删除测试INDEX
	if err!=nil{
		panic(err)
	}

}


