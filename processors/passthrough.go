package processors

import "github.com/DailyBurn/ratchet/data"

// Passthrough simply passes the data on to the next stage.
type Passthrough struct{}

func NewPassthrough() *Passthrough {
	return &Passthrough{}
}

func (r *Passthrough) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	outputChan <- d
}

func (r *Passthrough) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (r *Passthrough) String() string {
	return "Passthrough"
}
