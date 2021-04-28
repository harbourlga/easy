package library
//@author: [Harbourlga](https://github.com/Harbourlga)
import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/gookit/color"
	elastic "github.com/olivere/elastic/v7"
	"github.com/olivere/elastic/v7/config"
	"github.com/spf13/viper"
	"math"
	"strconv"
	"strings"
)


var Es = &_es{cfg: &config.Config{
	URL:         "",
	Index:       "",
	Username:    "",
	Password:    "",
	Shards:      0,
	Replicas:    0,
	Sniff:       nil,
	Healthcheck: nil,
	Infolog:     "",
	Errorlog:    "",
	Tracelog:    "",
}}


type _es struct {
	client *elastic.Client
	cfg *config.Config
	count int //插入数量
	insertFile string //插入文件
}



func (e *_es) Initialize() {
     e.cfg.URL = viper.GetString("ES.url")
     e.cfg.Index = viper.GetString("ES.indexName")
     e.insertFile = viper.GetString("FILE.filePath")
}

func (e *_es) MkConn() (error){
     client, err := elastic.NewClient(elastic.SetURL(e.cfg.URL),
     	elastic.SetBasicAuth(e.cfg.Username, e.cfg.Password),
     	elastic.SetSniff(false))
     if err!=nil{
     	return err
	 }
	 e.client = client
	 return nil
}

func (e *_es) Ping() (error) {
	_, _, err := e.client.Ping(e.cfg.URL).Do(context.Background())
	if err!=nil{
		return err
	}
	color.Info.Printf("connect %s success\n", e.cfg.URL)
	return nil
}

func (e *_es) Stop() {
	e.client.Stop()
}

func (e *_es) SearchIndex() ([]string, error) {
	indexName ,err:= e.client.IndexNames()
	if err!=nil{
		color.Warn.Println("Get Index Name Err: ", err)
		return nil, err
	}
	return indexName, err
}

func (e *_es) DeleteIndex () error {
	_, err := e.client.DeleteIndex(e.cfg.Index).Do(context.Background())
	if err!=nil{
		color.Warn.Println("Delete Err: ", err)
	}
	return err
}

func (e *_es) NewIndex () error {
	_, err := e.client.CreateIndex(e.cfg.Index).Do(context.Background())
	if err!=nil{
		color.Warn.Println("Create Index Err:", err)
	}
	color.Info.Println("Create Index Success")
	return err
}

func (e *_es) DocSearch (id string) (string, error) {
	result, err := e.client.Get().Index(e.cfg.Index).Id(id).Do(context.Background())
	if err!=nil{
		color.Warn.Println("Search Doc Err", err)
	}
	return string(result.Source), err
}

func (e *_es) DocCount () (int, bool, error){
	catCount, err := e.client.CatCount().Index(e.cfg.Index).Do(context.Background())  //计算目标INDEX的DOC数量
	if err!=nil{
		color.Warn.Println("Doc Count Err:", err)
		return 0, 0==e.count, err
	}
	if len(catCount)==1{
		return catCount[0].Count, catCount[0].Count==e.count, nil
	}else{
		color.Warn.Println("Doc Count Err:", err)
		return 0, 0==e.count, err
	}

}


//----------------------以下为插入数据的一些方法--------------------------
func (e *_es) Flush() error{
	_, err := e.client.Flush(Es.cfg.Index).Do(context.Background()) //刷脏数据
	if err!=nil{
		color.Warn.Println("Flush err")
	}
	return err
}

func (e *_es) InsertOneDocument(id string, doc string) error {
	_, err := e.client.Index().Index(e.cfg.Index).Id(id).BodyJson(doc).Do(context.Background())
	if err!=nil{
		color.Warn.Println("Insert One Doc Err: ", err)
	}
	color.Info.Println("Insert One Doc Success")
    return err
}

func (e *_es) InsertManyDocument(f func(string)([][]string, []string)) error {
	body, field := f(e.insertFile)
	//f func(string)[][]string, inputFile string
	//fmt.Println(body)
	length := len(body)
	fmt.Println("总长度：", length)
	e.count = length
	//var preRateLength int = 0
	//var rate string
	bulkRequest := e.client.Bulk()

	hax := md5.New()
	for i:=0; i<length;i++{
		hax.Reset()
		hax.Write([]byte(fmt.Sprint(i)))
		id := hex.EncodeToString(hax.Sum((nil)))
		//-----------拼接JSON字符串-------------
		var doc = `{`
		for k, f := range field{
			if k!=0{
				doc += `, `
			}
			doc += `"` + f + `": "` + body[i][k] + `"`
		}
		doc += `}`
		doc = strings.ReplaceAll(doc, "\n", "")

		//-----------拼接JSON字符串-------------
		//doc := fmt.Sprintf(`{"content": "狼性思维的核心之一是，解决不了问题，就先解决提出问题的人。当前加入第：%d数据"}`, i)
		// 添加进桶里
		req := elastic.NewBulkIndexRequest().Index(e.cfg.Index).Id(id).Doc(doc)
		bulkRequest = bulkRequest.Add(req)
		switch i%1000{
		case 0:
			//批量写入
			_, err :=bulkRequest.Do(context.Background())
			if err!=nil {
				color.Warn.Println( "bulk request error")
				return err
			}
		}

		//处理进度
		{
			rateNumber := math.Trunc((float64(i) / float64(length)) * 100)
			rateVal := strconv.FormatFloat(rateNumber, 'f', -1, 64) + "%"
			//for b:=0;b<preRateLength;b++{
			//	rate = string('\b') + rateVal
			//}
			//rate = string(strings.Repeat().Repeat('\b', uint(preRateLength)))

			//preRateLength = len(rateVal)
			h := strings.Repeat("=", int(rateNumber)) + strings.Repeat(" ", 100-int(rateNumber))
			color.Printf("\r%s%%[%s]", rateVal, h)
		}

	}
	//处理剩余
	switch bulkRequest.NumberOfActions() {
	case 0:
		color.Blueln("no remaining")
	default:
		_, err := bulkRequest.Do(context.Background())
		if err!=nil {
			color.Warn.Println( "bulk request error")
			return err
		}
	}
	{
		rateNumber := 100
		rateVal := "100%"
		h := strings.Repeat("=", int(rateNumber)) + strings.Repeat(" ", 100-int(rateNumber))
		color.Printf("\r%s%%[%s]", rateVal, h)
	}
	return nil
}