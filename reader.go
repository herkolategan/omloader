package omloader

import (
	"bufio"
	"errors"
	"io"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
	"github.com/prometheus/prometheus/prompb"
)

const readerBatchSize = 4096
const readerBufferSize = 4 * 1024 * 1024

type Reader interface {
	Read() error
}

type openMetricsReader struct {
	reader io.Reader
	output chan<- *prompb.TimeSeries
}

func NewOpenMetricsReader(reader io.Reader, output chan<- *prompb.TimeSeries) Reader {
	return &openMetricsReader{
		reader: reader,
		output: output,
	}
}

func (r *openMetricsReader) parseInput(input []byte) {
	p := textparse.NewOpenMetricsParser(input)
	for {
		entryType, err := p.Next()
		if entryType == textparse.EntryInvalid {
			break
		}
		if errors.Is(err, io.EOF) {
			break
		}

		switch entryType {
		case textparse.EntrySeries:
			_, timestamp, value := p.Series()
			var labels labels.Labels
			p.Metric(&labels)

			var timeSeries prompb.TimeSeries
			timeSeries.Labels = make([]prompb.Label, len(labels)+1)
			for i, labal := range labels {
				timeSeries.Labels[i] = prompb.Label{
					Name:  labal.Name,
					Value: labal.Value,
				}
			}

			// TODO(herko): Make this configurable
			// Add a monitor label to all time series.
			timeSeries.Labels[len(labels)] = prompb.Label{
				Name:  "monitor",
				Value: "gce-prom",
			}

			var sample prompb.Sample
			sample.Value = value
			if timestamp != nil {
				sample.Timestamp = *timestamp
			}
			timeSeries.Samples = []prompb.Sample{sample}
			r.output <- &timeSeries
		}
	}

}

func (r *openMetricsReader) Read() error {
	// TODO(herko): [perf] use seek and read in chucks, starting at new lines
	var batch []byte
	bufReader := bufio.NewReaderSize(r.reader, readerBufferSize)
	scanner := bufio.NewScanner(bufReader)
	scanner.Split(bufio.ScanLines)
	for i := 1; scanner.Scan(); i++ {
		batch = append(batch, scanner.Bytes()...)
		batch = append(batch, '\n')
		if i%readerBatchSize == 0 {
			r.parseInput(batch)
			batch = nil
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(batch) > 0 {
		r.parseInput(batch)
	}
	return nil
}
