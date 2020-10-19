package honeycomb

import (
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"

	"github.com/honeycombio/libhoney-go"
)

func init() {
	outputs.Add("honeycomb", func() telegraf.Output { return &Output{} })
}

type Output struct {
	APIKey         string   `toml:"apiKey"`
	Dataset        string   `toml:"dataset"`
	APIHost        string   `toml:"apiHost"`
	SpecialTags    []string `toml:"specialTags"`
	specialTagsMap map[string]struct{}
}

// Description returns the short description of the plugin.
func (o *Output) Description() string {
	return "Send telegraf metrics to Honeycomb.io"
}

// SampleConfig returns the sample configuration for the plugin.
func (o *Output) SampleConfig() string {
	return `
## Honeycomb authentication token
apiKey = "API_KEY"

## Dataset name in Honeycomb to send data to
dataset = "my-dataset"

## By default, every field and tag of a Metric are sent as a Honeycomb field
## prefixed by the metric name. So a Metric
## { name="disk" tags={device:sda} fields={free:232827793}}
## becomes two Honeycomb fields "disk.device" and "disk.free".
##
## Exclude tags from this behavor by setting them in this list. Any global tags
## should be included here. The "host" tag will always be treated as if it is
## included in this list.
# specialTags = []

## Optional: the hostname for the Honeycomb API server
# apiHost = "https://api.honeycomb.io/"
`
}

func (o *Output) Connect() error {
	if o.APIKey == "" {
		return errors.New("Honeycomb apiKey can not be empty")
	}

	if o.Dataset == "" {
		return errors.New("Honeycomb dataset can not be empty")
	}

	err := libhoney.Init(libhoney.Config{
		APIKey:       o.APIKey,
		Dataset:      o.Dataset,
		APIHost:      o.APIHost,
		MaxBatchSize: 500,
	})
	if err != nil {
		return fmt.Errorf("Honeycomb Init error: %s", err.Error())
	}

	o.specialTagsMap = make(map[string]struct{})
	for i := range o.SpecialTags {
		o.specialTagsMap[o.SpecialTags[i]] = struct{}{}
	}

	return nil
}

func (o *Output) Write(metrics []telegraf.Metric) error {
	// BuildEvent from metrics
	evs, err := o.BuildEvents(metrics)
	if err != nil {
		return fmt.Errorf("Honeycomb event creation error: %s", err.Error())
	}

	for _, ev := range evs {
		fmt.Printf("ev = %+v\n", ev)
		// send event
		if err = ev.Send(); err != nil {
			return fmt.Errorf("Honeycomb Send error: %s", err.Error())
		}
	}

	libhoney.Flush()

	return nil
}

func (o *Output) dataForMetric(m telegraf.Metric) map[string]interface{} {
	data := make(map[string]interface{})

	// add tags, by default prefixed with the metric name
	// do not prefix the special tag "host" or any tag listed in "special tags"
	for _, t := range m.TagList() {
		k := m.Name() + "." + t.Key
		if _, isSpecial := o.specialTagsMap[t.Key]; t.Key == "host" || isSpecial {
			k = t.Key
		}

		data[k] = t.Value
	}

	// add each field and value prefixed by metric / measurement name to data payload
	for _, f := range m.FieldList() {
		data[m.Name()+"."+f.Key] = f.Value
	}
	return data
}

func (o *Output) BuildEvents(ms []telegraf.Metric) ([]*libhoney.Event, error) {
	// For each timestamp, we want to send a single event to Honeycomb with as
	// many metrics as possible. However, some metrics may be sent to us more
	// than once. Eg, disk usage is sent once for each disk. So build a
	// map[time]map[name][]Metric. We'll then look at all the metric names for
	// a given timestamp that have only one value and batch them. Any metrics
	// that have > 1 value will be sent as separate events.
	metricsByTimeAndName := make(map[time.Time]map[string][]telegraf.Metric)

	for _, m := range ms {
		metricsByName := metricsByTimeAndName[m.Time()]
		if metricsByName == nil {
			metricsByName = make(map[string][]telegraf.Metric)
		}
		metricsByName[m.Name()] = append(metricsByName[m.Name()], m)
		metricsByTimeAndName[m.Time()] = metricsByName
	}

	var evs []*libhoney.Event
	for ts, metricsByName := range metricsByTimeAndName {
		// the single event for all the metrics flattened into a single event
		flatEvent := libhoney.NewEvent()
		flatEvent.Timestamp = ts

		// for each metric name with only 1 Metric, flatten it.
		// otherwise, create a unique event for it.
		for name, metrics := range metricsByName {
			if len(metrics) == 1 {
				fmt.Println("merging one:", name)
				if err := flatEvent.Add(o.dataForMetric(metrics[0])); err != nil {
					return nil, err
				}
			} else {
				fmt.Println("sending many:", name)

				// if the metrics with the same name result in a distinct set of field names,
				// we can still flatten them.
				if mergeable(metrics) {
					fmt.Println("yay! mergeable:", name)
					for i := range metrics {
						if err := flatEvent.Add(o.dataForMetric(metrics[i])); err != nil {
							return nil, err
						}
					}
				} else {
					for i := range metrics {
						ev := libhoney.NewEvent()
						ev.Timestamp = ts
						if err := ev.Add(o.dataForMetric(metrics[i])); err != nil {
							return nil, err
						}
						evs = append(evs, ev)
					}
				}
			}
		}

		// once we've processed all the events for this timestamp, we can add the flat event to the batch
		evs = append(evs, flatEvent)
	}

	return evs, nil
}

// mergeable returns true if the metrics can be merged into a single Honeycomb event
// without losing information. Specifically, this means that the metrics have
// disjoint fields an the same list of tags.
func mergeable(ms []telegraf.Metric) bool {
	var (
		fields = make(map[string]struct{})
		tags   []*telegraf.Tag
	)

	for i := range ms {
		if tags == nil {
			// Use the first set of tags we see as the canonical list to match
			tags = ms[i].TagList()
		} else {
			// Docs say that this returns ordered, so comparing without sorting
			// should be safe.
			for j, t := range ms[i].TagList() {
				if tags[j].Key != t.Key || tags[j].Value != t.Value {
					return false
				}
			}
		}

		// Check we've never seen any of the fields before.
		// Fields are unique by their metric name & field key, as we'll
		// concantenate the two when constructing the honeycomb event.
		for _, f := range ms[i].FieldList() {
			k := ms[i].Name() + f.Key
			if _, exists := fields[k]; exists {
				return false
			}
			fields[k] = struct{}{}
		}
	}

	return true
}

// Close ensures libhoney is closed.
func (h *Output) Close() error {
	libhoney.Close()
	return nil
}
