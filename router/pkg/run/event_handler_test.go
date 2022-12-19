package run_test

import (
	"errors"
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/lorenzoranucci/tor/router/pkg/run"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventHandler_OnRow_HappyPaths(t *testing.T) {
	type fields struct {
		eventDispatcher         *eventDispatcherMock
		aggregateIdColumnName   string
		aggregateTypeColumnName string
		payloadColumnName       string
	}
	type args struct {
		e *canal.RowsEvent
	}

	orderAggregateID := []byte("c44ade3e-9394-4e6e-8d2d-20707d61061c")
	orderAggregateType := []byte("order")
	orderPayload := []byte(`{"name": "new order"}`)
	orderOtherColumnValue := 11
	var timestamp uint32 = 5

	tests := []struct {
		name           string
		fields         fields
		args           args
		wantDispatches []run.OutboxEvent
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
			},
		},
		{
			name: "single row-events, with default column names",
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
								Name: "other_column",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
							orderPayload,
							orderOtherColumnValue,
						},
					},
					Header: &replication.EventHeader{
						Timestamp: timestamp,
					},
				},
			},
			fields: fields{
				eventDispatcher: &eventDispatcherMock{},
			},
			wantDispatches: []run.OutboxEvent{
				{
					AggregateID:   orderAggregateID,
					AggregateType: orderAggregateType,
					Payload:       orderPayload,
					Columns: []run.Column{
						{
							Name:  []byte("aggregate_id"),
							Value: orderAggregateID,
						},
						{
							Name:  []byte("aggregate_type"),
							Value: orderAggregateType,
						},
						{
							Name:  []byte("payload"),
							Value: orderPayload,
						},
						{
							Name:  []byte("other_column"),
							Value: []byte(`11`),
						},
					},
					EventTimestampFromDatabase: timestamp,
				},
			},
		},
		{
			name: "single row-event with custom column names and order",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "my_schema",
						Name:   "outbox",
						Columns: []schema.TableColumn{
							{
								Name: "aggregateType",
							},
							{
								Name: "payload_",
							},
							{
								Name: "aggregateId",
							},
							{
								Name: "other_column",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"order",
							orderPayload,
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							orderOtherColumnValue,
						},
					},
					Header: &replication.EventHeader{
						Timestamp: timestamp,
					},
				},
			},
			fields: fields{
				eventDispatcher:         &eventDispatcherMock{},
				aggregateIdColumnName:   "aggregateId",
				aggregateTypeColumnName: "aggregateType",
				payloadColumnName:       "payload_",
			},
			wantDispatches: []run.OutboxEvent{
				{
					AggregateID:   orderAggregateID,
					AggregateType: orderAggregateType,
					Payload:       orderPayload,
					Columns: []run.Column{
						{
							Name:  []byte("aggregateType"),
							Value: orderAggregateType,
						},
						{
							Name:  []byte("payload_"),
							Value: orderPayload,
						},
						{
							Name:  []byte("aggregateId"),
							Value: orderAggregateID,
						},
						{
							Name:  []byte("other_column"),
							Value: []byte(`11`),
						},
					},
					EventTimestampFromDatabase: timestamp,
				},
			},
		},
		{
			name: "multiple row-events, with default column names",
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
								Name: "other_column",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
							orderPayload,
							orderOtherColumnValue,
						},
						{
							"c38a5d13-788c-4878-8bdc-c012cbad5b82",
							"invoice",
							`{"name": "new invoice"}`,
							nil,
						},
					},
					Header: &replication.EventHeader{
						Timestamp: timestamp,
					},
				},
			},
			fields: fields{
				eventDispatcher: &eventDispatcherMock{},
			},
			wantDispatches: []run.OutboxEvent{
				{
					AggregateID:   orderAggregateID,
					AggregateType: orderAggregateType,
					Payload:       orderPayload,
					Columns: []run.Column{
						{
							Name:  []byte("aggregate_id"),
							Value: orderAggregateID,
						},
						{
							Name:  []byte("aggregate_type"),
							Value: orderAggregateType,
						},
						{
							Name:  []byte("payload"),
							Value: orderPayload,
						},
						{
							Name:  []byte("other_column"),
							Value: []byte(`11`),
						},
					},
					EventTimestampFromDatabase: timestamp,
				},
				{
					AggregateID:   []byte("c38a5d13-788c-4878-8bdc-c012cbad5b82"),
					AggregateType: []byte("invoice"),
					Payload:       []byte(`{"name": "new invoice"}`),
					Columns: []run.Column{
						{
							Name:  []byte("aggregate_id"),
							Value: []byte("c38a5d13-788c-4878-8bdc-c012cbad5b82"),
						},
						{
							Name:  []byte("aggregate_type"),
							Value: []byte("invoice"),
						},
						{
							Name:  []byte("payload"),
							Value: []byte(`{"name": "new invoice"}`),
						},
						{
							Name:  []byte("other_column"),
							Value: nil,
						},
					},
					EventTimestampFromDatabase: timestamp,
				},
			},
		},
		{
			name: "multiple row-events, with custom column names",
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
								Name: "aggregateType",
							},
							{
								Name: "payload_",
							},
							{
								Name: "otherColumn",
							},
						},
					},
					Action: canal.InsertAction,
					Rows: [][]interface{}{
						{
							"c44ade3e-9394-4e6e-8d2d-20707d61061c",
							"order",
							orderPayload,
							orderOtherColumnValue,
						},
						{
							"c38a5d13-788c-4878-8bdc-c012cbad5b82",
							"invoice",
							`{"name": "new invoice"}`,
							nil,
						},
					},
					Header: &replication.EventHeader{
						Timestamp: timestamp,
					},
				},
			},
			fields: fields{
				eventDispatcher:         &eventDispatcherMock{},
				aggregateIdColumnName:   "aggregateId",
				aggregateTypeColumnName: "aggregateType",
				payloadColumnName:       "payload_",
			},
			wantDispatches: []run.OutboxEvent{
				{
					AggregateID:   orderAggregateID,
					AggregateType: orderAggregateType,
					Payload:       orderPayload,
					Columns: []run.Column{
						{
							Name:  []byte("aggregateId"),
							Value: orderAggregateID,
						},
						{
							Name:  []byte("aggregateType"),
							Value: orderAggregateType,
						},
						{
							Name:  []byte("payload_"),
							Value: orderPayload,
						},
						{
							Name:  []byte("otherColumn"),
							Value: []byte(`11`),
						},
					},
					EventTimestampFromDatabase: timestamp,
				},
				{
					AggregateID:   []byte("c38a5d13-788c-4878-8bdc-c012cbad5b82"),
					AggregateType: []byte("invoice"),
					Payload:       []byte(`{"name": "new invoice"}`),
					Columns: []run.Column{
						{
							Name:  []byte("aggregateId"),
							Value: []byte("c38a5d13-788c-4878-8bdc-c012cbad5b82"),
						},
						{
							Name:  []byte("aggregateType"),
							Value: []byte("invoice"),
						},
						{
							Name:  []byte("payload_"),
							Value: []byte(`{"name": "new invoice"}`),
						},
						{
							Name:  []byte("otherColumn"),
							Value: nil,
						},
					},
					EventTimestampFromDatabase: timestamp,
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
			)
			require.NoError(t, err)

			if err := h.OnRow(tt.args.e); (err != nil) != false {
				t.Errorf("OnRow() error = %v, wantErr false", err)
			}

			assert.Equal(t, tt.wantDispatches, tt.fields.eventDispatcher.dispatches)
		})
	}
}

func TestEventHandler_OnRow_UnhappyPaths(t *testing.T) {
	type fields struct {
		eventDispatcher         *eventDispatcherMock
		aggregateIdColumnName   string
		aggregateTypeColumnName string
		payloadColumnName       string
	}
	type args struct {
		e *canal.RowsEvent
	}
	tests := []struct {
		name               string
		fields             fields
		args               args
		wantErrOnConstruct bool
	}{
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
			},
		},
		{
			name: "when a Column value is missing compared to table structure then error",
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
			},
		},
		{
			name: "when aggregate-id Column is missing then error",
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
			},
		},
		{
			name: "when aggregate-type Column is missing then error",
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
			},
		},
		{
			name: "when payload Column is missing then error",
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
			)
			if (err != nil) != tt.wantErrOnConstruct {
				t.Errorf("NewEventHandler() error = %v, wantErr %v", err, tt.wantErrOnConstruct)
			}

			if tt.wantErrOnConstruct {
				return
			}

			if err := h.OnRow(tt.args.e); (err != nil) != true {
				t.Errorf("OnRow() error = %v, wantErr true", err)
			}
		})
	}
}

type eventDispatcherMock struct {
	dispatches []run.OutboxEvent
	err        error
}

func (e *eventDispatcherMock) Dispatch(oe run.OutboxEvent) error {
	e.dispatches = append(e.dispatches, oe)

	return e.err
}
