package cmd

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/julienschmidt/httprouter"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var entityNotFound = errors.New("error not found")

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the http server",
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := NewDB()
		if err != nil {
			return err
		}

		err = initDatabase(db)
		if err != nil {
			return err
		}

		h := NewHTTPHandler(db)

		router := httprouter.New()
		router.PUT("/:uuid", h.Put)
		router.DELETE("/:uuid", h.Delete)

		return http.ListenAndServe(fmt.Sprintf(":%s", viper.GetString("apiPort")), router)
	},
}

func NewHTTPHandler(db *sqlx.DB) *HTTPHandler {
	return &HTTPHandler{db: db}
}

type HTTPHandler struct {
	db *sqlx.DB
}

type OrderEvent struct {
	Name string `json:"event,omitempty"`
	UUID string `json:"uuid,omitempty"`
}

func (h *HTTPHandler) Put(w http.ResponseWriter, _ *http.Request, ps httprouter.Params) {
	tx, err := h.db.Beginx()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	orderUUID := ps.ByName("uuid")

	//put business logic here

	if err := h.insertOrder(tx, orderUUID); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := h.insertEvent(tx, "created", orderUUID); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = tx.Commit()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *HTTPHandler) Delete(w http.ResponseWriter, _ *http.Request, ps httprouter.Params) {
	tx, err := h.db.Beginx()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	orderUUID := ps.ByName("uuid")
	//put business logic here

	err = h.deleteOrder(tx, orderUUID)
	if err != nil && errors.Is(err, entityNotFound) {
		err := tx.Rollback()
		if err != nil {
			logrus.Warn("transaction rollback failed")
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := h.insertEvent(tx, "deleted", orderUUID); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = tx.Commit()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *HTTPHandler) insertEvent(tx *sqlx.Tx, eventName string, orderUUID string) error {
	e := OrderEvent{
		Name: eventName,
		UUID: orderUUID,
	}

	payload, err := json.Marshal(&e)
	if err != nil {
		return err
	}

	query := `
INSERT INTO my_schema.my_outbox_table (uuid, aggregate_type, aggregate_id, payload)
VALUES (:uuid, 'order', :aggregate_id, :payload);`

	if _, err := tx.NamedExec(
		h.db.Rebind(query),
		map[string]interface{}{
			"uuid":         uuid.New().String(),
			"aggregate_id": orderUUID,
			"payload":      payload,
		},
	); err != nil {
		return err
	}

	return nil
}

func (h *HTTPHandler) insertOrder(tx *sqlx.Tx, orderUUID string) error {
	query := `INSERT INTO my_schema.order (uuid) VALUES (:uuid);`

	if _, err := tx.NamedExec(
		h.db.Rebind(query),
		map[string]interface{}{
			"uuid": orderUUID,
		},
	); err != nil {
		return err
	}

	return nil
}

func (h *HTTPHandler) deleteOrder(tx *sqlx.Tx, orderUUID string) error {
	query := `DELETE FROM my_schema.order WHERE uuid = :uuid;`

	res, err := tx.NamedExec(
		h.db.Rebind(query),
		map[string]interface{}{
			"uuid": orderUUID,
		},
	)
	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n == 0 {
		return entityNotFound
	}

	return nil
}

func NewDB() (*sqlx.DB, error) {
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/my_schema",
		viper.GetString("dbUser"),
		viper.GetString("dbPassword"),
		viper.GetString("dbHost"),
		viper.GetString("dbPort"),
	)
	return sqlx.Connect("mysql", dsn)
}

//go:embed create_outbox_table.sql
var createOutboxTableQuery string

//go:embed create_order_table.sql
var createOrderTableQuery string

func initDatabase(db *sqlx.DB) error {
	_, err := db.DB.Exec(createOutboxTableQuery)
	if err != nil {
		return err
	}
	_, err = db.DB.Exec(createOrderTableQuery)
	return err
}

func init() {
	viper.MustBindEnv("apiPort", "API_PORT")
	viper.MustBindEnv("dbHost", "DB_HOST")
	viper.MustBindEnv("dbPort", "DB_PORT")
	viper.MustBindEnv("dbUser", "DB_USER")
	viper.MustBindEnv("dbPassword", "DB_PASSWORD")

	rootCmd.AddCommand(runCmd)
}
