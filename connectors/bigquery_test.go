package connectors

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type Score struct {
	Name string
	Num  int
}

func (testSuit *bigquerySuite) SetupTest() {
	testSuit.bq.deleteTable(testSuit.DatasetName, testSuit.TableID)
}

func (testSuit *bigquerySuite) TestAddDataset() {
	testSuit.bq.AddDataSet(testSuit.DatasetName)
	datasetList, _ := testSuit.bq.listDatasets()

	// assert
	sort.Strings(datasetList)
	i := sort.SearchStrings(datasetList, testSuit.DatasetName)
	assert.True(testSuit.T(), i < len(datasetList) && datasetList[i] == testSuit.DatasetName, true)
}

func (testSuit *bigquerySuite) TestCreateTableIfNotExist() {

	testSuit.bq.AddDataSet(testSuit.DatasetName)

	scores := Score{Name: "n1", Num: 12}

	//schema, _ := bigquery.InferSchema(scores)
	testSuit.bq.CreateTableIfNotExist(0, scores, testSuit.TableID, testSuit.DatasetName)
	tableList, _ := testSuit.bq.listTables(testSuit.DatasetName)

	// assert
	sort.Strings(tableList)
	i := sort.SearchStrings(tableList, testSuit.TableID)
	assert.True(testSuit.T(), i < len(tableList) && tableList[i] == testSuit.TableID, true)
}

func (testSuit *bigquerySuite) TestSaveData() {

	testSuit.bq.AddDataSet(testSuit.DatasetName)

	scores := Score{Name: "n1", Num: 12}
	testSuit.bq.CreateTableIfNotExist(0, scores, testSuit.TableID, testSuit.DatasetName)

	scoresList := []Score{
		{Name: "n1", Num: 13},
		{Name: "n1", Num: 14},
		{Name: "n1", Num: 15},
		{Name: "n1", Num: 16},
		{Name: "n1", Num: 17},
		{Name: "n1", Num: 18},
	}

	testSuit.bq.SaveEvents(testSuit.DatasetName, testSuit.TableID, scoresList)
	amount, _ := testSuit.bq.browseTable(testSuit.DatasetName, testSuit.TableID)

	assert.True(testSuit.T(), amount > 1)
}

func TestBigquerySuite(t *testing.T) {
	testSuit := new(bigquerySuite)
	testSuit.bq = Instance("/tmp/bigquery-credentials/", "pjgg-157508")
	testSuit.DatasetName = "testdataset"
	testSuit.TableID = "test_table"
	suite.Run(t, testSuit)
}

type bigquerySuite struct {
	suite.Suite
	bq          *BigqueryConnector
	DatasetName string
	TableID     string
}
