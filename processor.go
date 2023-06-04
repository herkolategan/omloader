package omloader

import (
	"io"

	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
)

const processorBatchSize = 4096
const processorChannelSize = 8192

type Processor interface {
	Process(reader io.Reader) error
	AddMutator(mutator LabelMutatorFn)
}

type LabelMutatorFn func(labels []prompb.Label) []prompb.Label

type processor struct {
	logger   *zap.Logger
	sender   Sender
	mutators []LabelMutatorFn
}

func NewProcessor(logger *zap.Logger, sender Sender) Processor {
	return &processor{
		logger: logger,
		sender: sender,
	}
}

func (p *processor) Process(reader io.Reader) error {

	ch := make(chan *prompb.TimeSeries, processorChannelSize)
	omReader := NewOpenMetricsReader(reader, ch)

	go func() {
		defer close(ch)
		if err := omReader.Read(); err != nil {
			p.logger.Error("error reading file", zap.Error(err))
		}
	}()

	// TODO(herko): [perf] extend processor to group time series by labes and send it in batches / in parallel
	batch := make([]prompb.TimeSeries, 0, processorBatchSize)
	for ts := range ch {
		for _, mutator := range p.mutators {
			ts.Labels = mutator(ts.Labels)
		}
		batch = append(batch, *ts)
		if len(batch) == processorBatchSize {
			if err := p.sender.Send(batch); err != nil {
				return err
			}
			batch = make([]prompb.TimeSeries, 0, processorBatchSize)
		}
	}
	if len(batch) > 0 {
		if err := p.sender.Send(batch); err != nil {
			return err
		}
	}
	return nil
}

func (p *processor) AddMutator(mutator LabelMutatorFn) {
	p.mutators = append(p.mutators, mutator)
}
