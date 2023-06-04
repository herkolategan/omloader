package omloader

import (
	"bytes"
	"fmt"
	"net/http"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

type Sender interface {
	Send(timeSeriesSlice []prompb.TimeSeries) error
}

type HttpSenderConfig struct {
	URL string
}

type httpSender struct {
	HttpSenderConfig
}

func NewHttpSender(config HttpSenderConfig) Sender {
	return &httpSender{
		HttpSenderConfig: config,
	}
}

func (c *httpSender) Send(timeSeriesSlice []prompb.TimeSeries) error {
	writeRequest := &prompb.WriteRequest{
		Timeseries: timeSeriesSlice,
	}

	wrBuf, err := writeRequest.Marshal()
	if err != nil {
		return err
	}

	buf := snappy.Encode(nil, wrBuf)
	req, err := http.NewRequest("POST", c.URL, bytes.NewBuffer(buf))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}

	return nil
}
