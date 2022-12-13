package cmd

import (
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-redis/redis/v8"
	"github.com/lorenzoranucci/tor/adapters/kafka"
	redis2 "github.com/lorenzoranucci/tor/adapters/redis"
	"github.com/lorenzoranucci/tor/router/pkg/run"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the application",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := canal.NewCanal(getCanalConfig())
		if err != nil {
			return err
		}

		ed, err := getKafkaEventDispatcher()
		if err != nil {
			return err
		}

		var aggregateTypeRegexp *regexp.Regexp
		if r := viper.GetString("aggregateTypeRegexp"); r != "" {
			aggregateTypeRegexp, err = regexp.Compile(r)
			if err != nil {
				return err
			}
		}

		stateHandler := getRedisStateHandler()
		handler, err := run.NewEventHandler(
			ed,
			viper.GetString("dbAggregateIDColumnName"),
			viper.GetString("dbAggregateTypeColumnName"),
			viper.GetString("dbPayloadColumnName"),
			viper.GetStringSlice("dbHeadersColumnsNames"),
			aggregateTypeRegexp,
		)
		if err != nil {
			return err
		}

		runner := run.NewRunner(c, handler, stateHandler, time.Second*5)

		return runner.Run()
	},
}

func init() {
	viper.MustBindEnv("dbHost", "DB_HOST")
	viper.MustBindEnv("dbPort", "DB_PORT")
	viper.MustBindEnv("dbUser", "DB_USER")
	viper.MustBindEnv("dbPassword", "DB_PASSWORD")
	viper.MustBindEnv("dbOutboxTableRef", "DB_OUTBOX_TABLE_REF")
	viper.MustBindEnv("dbAggregateIDColumnName", "DB_AGGREGATE_ID_COLUMN_NAME")
	viper.MustBindEnv("dbAggregateTypeColumnName", "DB_AGGREGATE_TYPE_COLUMN_NAME")
	viper.MustBindEnv("dbPayloadColumnName", "DB_PAYLOAD_COLUMN_NAME")
	viper.MustBindEnv("dbHeadersColumnsNames", "DB_HEADERS_COLUMNS_NAME")
	viper.MustBindEnv("aggregateTypeRegexp", "AGGREGATE_TYPE_REGEXP_EXPRESSION")

	viper.MustBindEnv("kafkaBrokers", "KAFKA_BROKERS")
	viper.MustBindEnv("kafkaTopic", "KAFKA_TOPIC")

	viper.MustBindEnv("redisHost", "REDIS_HOST")
	viper.MustBindEnv("redisPort", "REDIS_PORT")
	viper.MustBindEnv("redisDB", "REDIS_DB")
	viper.MustBindEnv("redisKey", "REDIS_KEY")

	// Configure logrus for tor/router
	logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.InfoLevel)

	rootCmd.AddCommand(runCmd)
}

func getKafkaEventDispatcher() (*kafka.EventDispatcher, error) {
	producer, err := kafka.NewProducer(
		viper.GetStringSlice("kafkaBrokers"),
		viper.GetString("kafkaTopic"),
	)
	if err != nil {
		return nil, err
	}

	return kafka.NewEventDispatcher(producer), nil
}

func getRedisStateHandler() *redis2.StateHandler {
	return redis2.NewStateHandler(
		redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("%s:%s", viper.GetString("redisHost"), viper.GetString("redisPort")),
			DB:   viper.GetInt("redisDB"),
		}),
		viper.GetString("redisKey"),
	)
}

func getCanalConfig() *canal.Config {
	cfg := canal.NewDefaultConfig()

	cfg.Addr = fmt.Sprintf("%s:%s", viper.GetString("dbHost"), viper.GetString("dbPort"))
	cfg.User = viper.GetString("dbUser")
	cfg.Password = viper.GetString("dbPassword")
	cfg.Dump.ExecutionPath = ""
	cfg.IncludeTableRegex = []string{fmt.Sprintf("^%s$", viper.Get("dbOutboxTableRef"))}
	cfg.MaxReconnectAttempts = 10
	cfg.ReadTimeout = time.Second * 10

	return cfg
}
