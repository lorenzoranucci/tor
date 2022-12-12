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
		eventDispatcher         *eventDispatcherMock
		aggregateIdColumnName   string
		aggregateTypeColumnName string
		payloadColumnName       string
		aggregateTypeRegexp     *regexp.Regexp
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
			},
			wantDispatches: []dispatch{
				{
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
			},
			wantDispatches: []dispatch{
				{
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
			},
			wantDispatches: []dispatch{
				{
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
			},
			wantDispatches: []dispatch{
				{
					routingKey: "c44ade3e-9394-4e6e-8d2d-20707d61061c",
					event:      []byte(`{"name": "new order"}`),
				},
				{
					routingKey: "c38a5d13-788c-4878-8bdc-c012cbad5b82",
					event:      []byte(`{"name": "new order"}`),
				},
			},
		},
		{
			name: "multiple row-events, with entry not matching aggregate-type regexp",
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
							`{"name": "new order"}`,
						},
					},
					Header: &replication.EventHeader{},
				},
			},
			fields: fields{
				eventDispatcher:     &eventDispatcherMock{},
				aggregateTypeRegexp: regexp.MustCompile("(?i)^order$"),
			},
			wantDispatches: []dispatch{
				{
					routingKey: "c44ade3e-9394-4e6e-8d2d-20707d61061c",
					event:      []byte(`{"name": "new order"}`),
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
				tt.fields.aggregateTypeRegexp,
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
		aggregateTypeRegexp     *regexp.Regexp
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
			},
			wantDispatches: []dispatch{
				{
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
				tt.fields.aggregateTypeRegexp,
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
	routingKey string
	event      []byte
}

type eventDispatcherMock struct {
	dispatches []dispatch
	err        error
}

func (e *eventDispatcherMock) Dispatch(routingKey string, event []byte) error {
	e.dispatches = append(e.dispatches, dispatch{
		routingKey: routingKey,
		event:      event,
	})

	return e.err
}
