package service

import (
	"encoding/json"
	"fmt"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/flight"
	stats "github.com/dgryski/go-onlinestats"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	arrowRecBufSize    = 10000
	gaRecBufSize       = 20000
	coExprBufSize      = 20000
	numSpearmanWorkers = 16
)

type CoExpressionService struct{}

func NewCoExpressionService() *CoExpressionService {
	return &CoExpressionService{}
}

func (s *CoExpressionService) fetchCoExpressions(w http.ResponseWriter, r ServiceRequest) error {

	query := fmt.Sprintf("SELECT * from \"expressionService\".\"%s\"", r.MolecularProfileA)
	flightReader, err := arrowFlightFetch(query)
	if err != nil {
		return err
	}

	arrowRecCh := make(chan array.Record, arrowRecBufSize)
	gaRecCh := make(chan []GeneticAlteration, gaRecBufSize)
	mapCompCh := make(chan struct{})
	coExprCh := make(chan CoExpression, coExprBufSize)
	entityValues := make(map[int64][]float64)
	start := time.Now()

	go fetchRecords(flightReader, arrowRecCh)
	go transformRecToGeneticAlteration(arrowRecCh, gaRecCh)
	go mapValuesToEntrezId(gaRecCh, mapCompCh, entityValues)
	go computeSpearman(mapCompCh, coExprCh, r.Entrez_Gene_id, entityValues)

	var (
		wroteFirst = false
		enc        = json.NewEncoder(w)
		comma      = []byte(",")
	)
	w.Write([]byte("["))
	for coexpr := range coExprCh {
		if wroteFirst {
			w.Write(comma)
		} else {
			wroteFirst = true
		}
		enc.Encode(coexpr)
	}
	w.Write([]byte("]\n"))

	elapsed := time.Since(start)
	log.Printf("[INFO] total fetchCoExpression time: %s\n", elapsed)

	return nil
}

func fetchRecords(flightReader *flight.Reader, arrowRecCh chan array.Record) {

	start := time.Now()
	var elapsed time.Duration
	defer close(arrowRecCh)
	for flightReader.Next() {
		rec := flightReader.Record()
		rec.Retain()
		arrowRecCh <- rec
	}
	elapsed = time.Since(start)
	log.Printf("[INFO] total Dremio time: %s\n", elapsed)
}

func transformRecToGeneticAlteration(arrowRecCh chan array.Record, gaRecCh chan []GeneticAlteration) {
	var rowsFetched int64 = 0
	for rec := range arrowRecCh {
		rowsFetched += rec.NumRows()
		alterations := make([]GeneticAlteration, 0, rec.NumRows())
		for i := 0; i < int(rec.NumRows()); i++ {
			// for each row
			alt := GeneticAlteration{}
			for j := 0; j < int(rec.NumCols()); j++ {
				// loop over column and create struct
				switch rec.ColumnName(j) {
				case "Entrez_Gene_Identifier":
					alt.Entrez_Gene_Identifier = rec.Column(j).(*array.Int64).Value(i)
				case "Sample_Identifier":
					alt.Sample_Identifier = rec.Column(j).(*array.String).Value(i)
				case "Value":
					alt.Value = rec.Column(j).(*array.Float32).Value(i)
				}
			}
			alterations = append(alterations, alt)
		}
		gaRecCh <- alterations
		// release manually instead of defer so that it happens now instead of waiting
		// till the end of the function. keeping our memory usage down.
		rec.Release()
	}
	log.Printf("[INFO] total number of rows: %d\n", rowsFetched)
	close(gaRecCh)
}

func mapValuesToEntrezId(gaRecCh chan []GeneticAlteration, mapCompCh chan struct{}, entityValues map[int64][]float64) {
	for gas := range gaRecCh {
		for _, ga := range gas {
			valueSlice := entityValues[ga.Entrez_Gene_Identifier]
			if valueSlice == nil {
				valueSlice = make([]float64, 0, 2000) // large enough for metabric samples, this should be computed
			}
			valueSlice = append(valueSlice, float64(ga.Value))
			entityValues[ga.Entrez_Gene_Identifier] = valueSlice
		}
	}
	mapCompCh <- struct{}{}
}

func computeSpearman(mapCompCh chan struct{}, coExprCh chan CoExpression, queryGene int64, entityValues map[int64][]float64) {

	<-mapCompCh // wait for mapping stage to complete

	var wg sync.WaitGroup
	var tokens = make(chan struct{}, numSpearmanWorkers)
	queryValues := entityValues[queryGene]
	for entrezId, values := range entityValues {
		if entrezId == queryGene {
			continue
		}
		wg.Add(1)
		tokens <- struct{}{}
		go func(entrezId int64, values []float64) {
			defer wg.Done()
			rank, p := stats.Spearman(values, queryValues)
			coexp := CoExpression{
				Genetic_Entity_Identifier: strconv.FormatInt(entrezId, 10),
				Spearman:                  rank,
				PValue:                    p,
			}
			coExprCh <- coexp
			<-tokens
		}(entrezId, values) // queryValues does not change, no need to pass
	}
	wg.Wait()
	close(coExprCh)
}
