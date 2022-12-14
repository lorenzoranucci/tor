package run_test

import (
	"errors"
	"reflect"
	"regexp"
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/lorenzoranucci/tor/router/pkg/run"
	"github.com/stretchr/testify/require"
)

func TestEventHandler_OnRow_HappyPaths(t *testing.T) {
	type fields struct {
		eventDispatcher             *eventDispatcherMock
		aggregateIdColumnName       string
		aggregateTypeColumnName     string
		payloadColumnName           string
		headersColumnsNames         []string
		aggregateTypeTopicPairs     []run.AggregateTypeTopicPair
		includeTransactionTimestamp bool
	}
	type args struct {
		e *canal.RowsEvent
	}
	tests := []struct {
		name           string
		fields         fields
		args           args
		wantDispatches []dispatch
		wantErr        bool
	}{
		{
			name: "no topic-aggregate type pair then no dispatch",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
							`{"name": "new order"}`,
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher: &eventDispatcherMock{},
			},
		},
		{
			name: "single row-event, with string payload, and default column ids",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
							`{"name": "new order"}`,
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher: &eventDispatcherMock{},
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
			wantDispatches: []dispatch{
				{
					topic:      "order",
					routingKey: "c44ade3e-9394-4e6e-8d2d-20707d61061c",
					event:      []byte(`{"name": "new order"}`),
				},
			},
		},
		{
			name: "single row-event, with byte payload, and default column ids",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
							[]byte(`{"name": "new order"}`),
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher: &eventDispatcherMock{},
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
			wantDispatches: []dispatch{
				{
					topic:      "order",
					routingKey: "c44ade3e-9394-4e6e-8d2d-20707d61061c",
					event:      []byte(`{"name": "new order"}`),
				},
			},
		},
		{
			name: "single row-event, custom column ids",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "id",
							},
							{
								Name: "aggregateType",
							},
							{
								Name: "payload_",
							},
							{
								Name: "aggregateId",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							1,
							"order",
							`{"name": "new order"}`,
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher:         &eventDispatcherMock{},
				aggregateIdColumnName:   "aggregateId",
				aggregateTypeColumnName: "aggregateType",
				payloadColumnName:       "payload_",
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
			wantDispatches: []dispatch{
				{
					topic:      "order",
					routingKey: "c44ade3e-9394-4e6e-8d2d-20707d61061c",
					event:      []byte(`{"name": "new order"}`),
				},
			},
		},
		{
			name: "multiple row-events, with default column ids",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
							[]byte(`{"name": "new order"}`),
						},
						{
							"c38a5d13-788c-4878-8bdc-c012cbad5b82",
							"order",
							`{"name": "new order"}`,
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher: &eventDispatcherMock{},
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
			wantDispatches: []dispatch{
				{
					topic:      "order",
					routingKey: "c44ade3e-9394-4e6e-8d2d-20707d61061c",
					event:      []byte(`{"name": "new order"}`),
				},
				{
					topic:      "order",
					routingKey: "c38a5d13-788c-4878-8bdc-c012cbad5b82",
					event:      []byte(`{"name": "new order"}`),
				},
			},
		},
		{
			name: "multiple row-events, with one entry not matching aggregate-type regexp",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
							[]byte(`{"name": "new order"}`),
						},
						{
							"c38a5d13-788c-4878-8bdc-c012cbad5b82",
							"invoice",
							`{"name": "new invoice"}`,
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher: &eventDispatcherMock{},
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
			wantDispatches: []dispatch{
				{
					topic:      "order",
					routingKey: "c44ade3e-9394-4e6e-8d2d-20707d61061c",
					event:      []byte(`{"name": "new order"}`),
				},
			},
		},
		{
			name: "multiple row-events, with different matching aggregate-types regexp",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
							[]byte(`{"name": "new order"}`),
						},
						{
							"c38a5d13-788c-4878-8bdc-c012cbad5b82",
							"invoice",
							`{"name": "new invoice"}`,
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher: &eventDispatcherMock{},
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^invoice$"),
						Topic:               "invoice",
					},
				},
			},
			wantDispatches: []dispatch{
				{
					topic:      "order",
					routingKey: "c44ade3e-9394-4e6e-8d2d-20707d61061c",
					event:      []byte(`{"name": "new order"}`),
				},
				{
					topic:      "invoice",
					routingKey: "c38a5d13-788c-4878-8bdc-c012cbad5b82",
					event:      []byte(`{"name": "new invoice"}`),
				},
			},
		},
		{
			name: "single row-event, with headers",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "counter",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload",
							},
							{
								Name: "uuid",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							1,
							"order",
							`{"name": "new order"}`,
							"b948f9a6-5797-4585-b386-dd8a1a4e30db",
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher:     &eventDispatcherMock{},
				headersColumnsNames: []string{"uuid", "counter"},
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
			wantDispatches: []dispatch{
				{
					topic:      "order",
					routingKey: "c44ade3e-9394-4e6e-8d2d-20707d61061c",
					event:      []byte(`{"name": "new order"}`), headers: []struct {
						Key   []byte
						Value []byte
					}{
						{
							Key:   []byte("uuid"),
							Value: []byte("b948f9a6-5797-4585-b386-dd8a1a4e30db"),
						},
						{
							Key:   []byte("counter"),
							Value: []byte("1"),
						},
					},
				},
			},
		},
		{
			name: "single row-event, with transaction timestamp",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "counter",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload",
							},
							{
								Name: "uuid",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							1,
							"order",
							`{"name": "new order"}`,
							"b948f9a6-5797-4585-b386-dd8a1a4e30db",
						},
					},
					Header: &replication.EventHeader{
						Timestamp: 100,
					},
				},
			},
			fields: fields{
				eventDispatcher:             &eventDispatcherMock{},
				includeTransactionTimestamp: true,
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
			wantDispatches: []dispatch{
				{
					topic:      "order",
					routingKey: "c44ade3e-9394-4e6e-8d2d-20707d61061c",
					event:      []byte(`{"name": "new order"}`), headers: []struct {
						Key   []byte
						Value []byte
					}{
						{
							Key:   []byte("transactionTimestamp"),
							Value: []byte("100"),
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			h, err := run.NewEventHandler(
				tt.fields.eventDispatcher,
				tt.fields.aggregateIdColumnName,
				tt.fields.aggregateTypeColumnName,
				tt.fields.payloadColumnName,
				tt.fields.headersColumnsNames,
				tt.fields.aggregateTypeTopicPairs,
				tt.fields.includeTransactionTimestamp,
			)
			require.NoError(t, err)

			if err := h.OnRow(tt.args.e); (err != nil) != tt.wantErr {
				t.Errorf("OnRow() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !reflect.DeepEqual(tt.wantDispatches, tt.fields.eventDispatcher.dispatches) {
				t.Errorf("OnRow() dispatches = %v, wantDispatches %v", tt.fields.eventDispatcher.dispatches, tt.wantDispatches)
			}
		})
	}
}

func TestEventHandler_OnRow_UnhappyPaths(t *testing.T) {
	type fields struct {
		eventDispatcher         *eventDispatcherMock
		aggregateIdColumnName   string
		aggregateTypeColumnName string
		payloadColumnName       string
		headersColumnsNames     []string
		aggregateTypeTopicPairs []run.AggregateTypeTopicPair
	}
	type args struct {
		e *canal.RowsEvent
	}
	tests := []struct {
		name               string
		fields             fields
		args               args
		wantDispatches     []dispatch
		wantErr            bool
		wantErrOnConstruct bool
	}{
		{
			name: "when row-event action is not insert then row-event is skipped",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload",
							},
						},
					},
					Action: canal.DeleteAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
							`{"name": "new order"}`,
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher: &eventDispatcherMock{},
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
		},
		{
			name: "when dispatcher fails then error",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
							`{"name": "new order"}`,
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher: &eventDispatcherMock{
					err: errors.New(""),
				},
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
			wantDispatches: []dispatch{
				{
					topic:      "order",
					routingKey: "c44ade3e-9394-4e6e-8d2d-20707d61061c",
					event:      []byte(`{"name": "new order"}`),
				},
			},
			wantErr: true,
		},
		{
			name: "when a column value is missing then error",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher: &eventDispatcherMock{},
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "when a header's column value is missing then error",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload",
							},
							{
								Name: "uuid",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
							"",
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher:     &eventDispatcherMock{},
				headersColumnsNames: []string{"uuid"},
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "when aggregate-id column is missing then error",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregateId",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
							`{"name": "new order"}`,
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher: &eventDispatcherMock{},
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "when aggregate-type column is missing then error",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "aggregateType",
							},
							{
								Name: "payload",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
							`{"name": "new order"}`,
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher: &eventDispatcherMock{},
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "when payload column is missing then error",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload_",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
							`{"name": "new order"}`,
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher: &eventDispatcherMock{},
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "when header column is missing then error",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
							`{"name": "new order"}`,
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher:     &eventDispatcherMock{},
				headersColumnsNames: []string{"uuid"},
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "when aggregate-id value is not string then error",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							1,
							"order",
							`{"name": "new order"}`,
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher: &eventDispatcherMock{},
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "when aggregate-type value is not string then error",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							1,
							`{"name": "new order"}`,
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher: &eventDispatcherMock{},
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "when payload value is not string then error",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregate_id",
							},
							{
								Name: "aggregate_type",
							},
							{
								Name: "payload",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
							1,
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher: &eventDispatcherMock{},
				aggregateTypeTopicPairs: []run.AggregateTypeTopicPair{
					{
						AggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
						Topic:               "order",
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			h, err := run.NewEventHandler(
				tt.fields.eventDispatcher,
				tt.fields.aggregateIdColumnName,
				tt.fields.aggregateTypeColumnName,
				tt.fields.payloadColumnName,
				tt.fields.headersColumnsNames,
				tt.fields.aggregateTypeTopicPairs,
				false,
			)
			if (err != nil) != tt.wantErrOnConstruct {
				t.Errorf("NewEventHandler() error = %v, wantErr %v", err, tt.wantErrOnConstruct)
			}

			if tt.wantErrOnConstruct {
				return
			}

			if err := h.OnRow(tt.args.e); (err != nil) != tt.wantErr {
				t.Errorf("OnRow() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !reflect.DeepEqual(tt.wantDispatches, tt.fields.eventDispatcher.dispatches) {
				t.Errorf("OnRow() dispatches = %v, wantDispatches %v", tt.fields.eventDispatcher.dispatches, tt.wantDispatches)
			}
		})
	}
}

type dispatch struct {
	topic      string
	routingKey string
	event      []byte
	headers    []struct {
		Key   []byte
		Value []byte
	}
}

type eventDispatcherMock struct {
	dispatches []dispatch
	err        error
}

func (e *eventDispatcherMock) Dispatch(topic string, routingKey string, event []byte, headers []struct {
	Key   []byte
	Value []byte
}) error {
	e.dispatches = append(e.dispatches, dispatch{
		topic:      topic,
		routingKey: routingKey,
		event:      event,
		headers:    headers,
	})

	return e.err
}
