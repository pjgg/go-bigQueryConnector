package connectors

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path"
	"reflect"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Sirupsen/logrus"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type BigqueryConnector struct {
	client  *bigquery.Client
	ctx     context.Context
	dataset map[string]*bigquery.Dataset
}

type BigqueryConnectorBehavior interface {
	SaveEvents(datasetID, tableID string, event ...interface{})
	CreateTableIfNotExist(dataExpiration int64, schema interface{}, tableID, datasetID string)
	AddDataSet() error
	listDatasets() ([]string, error)
	listTables(datasetID string) ([]string, error)
	browseTable(datasetID, tableID string) (int, error)
	copyTable(datasetID, srcID, dstID string) error
	deleteTable(datasetID, tableID string) error
	importFromFile(datasetID, tableID, filename string) error
	importFromGCS(datasetID, tableID, gcsURI string) error
	exportToGCS(datasetID, tableID, gcsURI string) error
}

var onceBigquery sync.Once
var bigQueryConnector BigqueryConnector

func Instance(credsPath, projectID string) *BigqueryConnector {
	onceBigquery.Do(func() {
		ctx := context.Background()
		if credsPath != "" {
			jsonKey, err := ioutil.ReadFile(path.Join(credsPath, "keyfile.json"))

			if err != nil {
				log.Fatal(err.Error())
			}

			conf, err := google.JWTConfigFromJSON(
				jsonKey,
				bigquery.Scope,
			)

			if err != nil {
				log.Fatal(err.Error())
			}

			client, err := bigquery.NewClient(
				ctx,
				projectID,
				option.WithTokenSource(conf.TokenSource(ctx)),
			)

			bigQueryConnector.dataset = make(map[string]*bigquery.Dataset)

			bigQueryConnector.client = client
			bigQueryConnector.ctx = ctx

			if err != nil {
				log.Fatal("NewBigQueryConnector" + err.Error())
			}
		} else {
			log.Fatal("Missing Bigquery credentials path")
		}

		bigquery.NewClient(ctx, projectID)
	})

	return &bigQueryConnector
}

func (b *BigqueryConnector) SaveEvents(datasetID, tableID string, event ...interface{}) (err error) {
	u := b.dataset[datasetID].Table(tableID).Uploader()
	if len(event) > 0 {
		if err = u.Put(b.ctx, event[0]); err != nil {
			logrus.Error(err)
		}
	}
	return
}

func (b *BigqueryConnector) CreateTableIfNotExist(dataExpiration int64, schema interface{}, tableID, datasetID string) (err error) {

	schemaType := reflect.New(reflect.TypeOf(schema))
	schemaInfered, err := bigquery.InferSchema(schemaType.Interface())

	partition := &bigquery.TimePartitioning{
		Expiration: time.Duration(dataExpiration) * time.Second,
	}

	table := b.dataset[datasetID].Table(tableID)
	if err = table.Create(b.ctx, &bigquery.TableMetadata{Schema: schemaInfered, TimePartitioning: partition}); err != nil {
		logrus.Warn("Failed to create table: %v", err)
		err = nil
	}

	return
}

func (b *BigqueryConnector) AddDataSet(datasetID string) (err error) {
	b.dataset[datasetID] = b.client.Dataset(datasetID)

	if err = b.dataset[datasetID].Create(b.ctx, &bigquery.DatasetMetadata{Name: datasetID}); err != nil {
		logrus.Warn("Failed to create dataset: %v", err)
		err = nil
	}

	return
}

func (b *BigqueryConnector) listDatasets() (result []string, err error) {

	it := b.client.Datasets(b.ctx)
	for {
		dataset, err := it.Next()
		if err == iterator.Done {
			break
		}
		logrus.Info(dataset.DatasetID)
		result = append(result, dataset.DatasetID)
	}

	return
}

func (b *BigqueryConnector) listTables(datasetID string) (result []string, err error) {

	ts := b.dataset[datasetID].Tables(b.ctx)
	for {
		t, err := ts.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		logrus.Info("Table: %q\n", t.TableID)
		result = append(result, t.TableID)
	}

	return
}

func (b *BigqueryConnector) browseTable(datasetID, tableID string) (amount int, err error) {

	table := b.dataset[datasetID].Table(tableID)
	it := table.Read(b.ctx)
	for {
		var row []bigquery.Value
		err = it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}
		amount++
		logrus.Info(row)
	}

	return
}

func (b *BigqueryConnector) copyTable(datasetID, srcID, dstID string) error {

	copier := b.dataset[datasetID].Table(dstID).CopierFrom(b.dataset[datasetID].Table(srcID))
	copier.WriteDisposition = bigquery.WriteTruncate
	job, err := copier.Run(b.ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(b.ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}

	return nil
}

func (b *BigqueryConnector) deleteTable(datasetID, tableID string) error {

	table := b.dataset[datasetID].Table(tableID)
	if err := table.Delete(b.ctx); err != nil {
		return err
	}

	return nil
}

func (b *BigqueryConnector) importFromGCS(datasetID, tableID, gcsURI string) error {

	// For example, "gs://data-bucket/path/to/data.csv"
	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.AllowJaggedRows = true
	// TODO: set other options on the GCSReference.

	loader := b.dataset[datasetID].Table(tableID).LoaderFrom(gcsRef)
	loader.CreateDisposition = bigquery.CreateNever
	// TODO: set other options on the Loader.

	job, err := loader.Run(b.ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(b.ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}

	return nil
}

func (b *BigqueryConnector) importFromFile(datasetID, tableID, filename string) error {

	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	source := bigquery.NewReaderSource(f)
	source.AllowJaggedRows = true
	// TODO: set other options on the GCSReference.

	loader := b.dataset[datasetID].Table(tableID).LoaderFrom(source)
	loader.CreateDisposition = bigquery.CreateNever
	// TODO: set other options on the Loader.

	job, err := loader.Run(b.ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(b.ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}

	return nil
}

func (b *BigqueryConnector) exportToGCS(datasetID, tableID, gcsURI string) error {

	// For example, "gs://data-bucket/path/to/data.csv"
	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.FieldDelimiter = ","

	extractor := b.dataset[datasetID].Table(tableID).ExtractorTo(gcsRef)
	extractor.DisableHeader = true
	job, err := extractor.Run(b.ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(b.ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}
	// [END bigquery_export_gcs]
	return nil
}
