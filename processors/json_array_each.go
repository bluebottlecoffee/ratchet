package processors

import (
	"encoding/json"

	"github.com/bluebottlecoffee/ratchet/data"
)

type JSONArrayEach struct {
	BatchSize        int
	ConcurrencyLevel int
}

func NewJSONArrayEach(batchSize int) *JSONArrayEach {
	return &JSONArrayEach{ConcurrencyLevel: 5, BatchSize: batchSize}
}

func (w *JSONArrayEach) Finish(outputChan chan data.JSON, killChan chan error) {}
func (w *JSONArrayEach) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	rows := make([]json.RawMessage, w.BatchSize)

	err := data.ParseJSON(d, &rows)

	if err != nil {
		killChan <- err
	}

	for _, row := range rows {
		outputChan <- data.JSON(row)
	}
}
func (w *JSONArrayEach) String() string {
	return "JSONArrayEach"
}

func (w *JSONArrayEach) Concurrency() int {
	return w.ConcurrencyLevel
}
