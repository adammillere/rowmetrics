package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"gopkg.in/yaml.v2"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

// applicationConfig is the struct which the config YAML will be mapped to
// To see an example, look at config.yml.example
type applicationConfig struct {
	AwsConfig map[string]string `yaml:"aws"`
	CountPath string            `yaml:"countPath"`
	Databases []databaseConfig
}

// countConfig is the struct which the counts YAML will be mapped to and written as
// To see an example, look at counts.yml.example
type countConfig struct {
	counts []countCollection
}

// databaseConfig is the struct which represents all information to obtain RowMetrics
// To see an example, see the "databases" configuration in config.yml.example
type databaseConfig struct {
	Name     string
	Host     string
	Type     string
	User     string
	Password string
	Database string
	Schema   string
	Tables   tableConfig
}

// tableConfig is two collections of table names that will have RowMetrics obtained for them
// Increment are tables that will have their current AUTO_INCREMENT pushed as the metric
// Row are tables that will have their approximate row count pushed as a metric (less accurate)
type tableConfig struct {
	Increment []string
	Row       []string
}

// countCollection is a collection of table names and their counts
// Increment are tables that will have their current AUTO_INCREMENT pushed as the metric
// Row are tables that will have their approximate row count pushed as a metric (less accurate)
// Example: Increment["RequestLog"] := 4000
type countCollection struct {
	Increment map[string]int
	Row       map[string]int
}

func main() {
	// Load application config as flag if specified, otherwise, use config.yml in current workdir
	var configPath string
	flag.StringVar(&configPath, "config", "config.yml", "path to the application config YAML file")
	flag.Parse()

	// Load application configuration
	config, err := loadApplicationConfig(configPath)
	if err != nil {
		log.Panicf("FATAL: Failed to load application config YAML: %s", err)
	}

	// Create the countCollections map that represents the current values to be grabbed
	var curCountCollections map[string]countCollection
	curCountCollections = make(map[string]countCollection)

	for _, database := range config.Databases {
		// Go through each configured database
		// Obtain the countCollection for this database
		curCountCollection, err := getCountCollection(database)
		if err != nil {
			log.Panicf("FATAL: Failed to get counts for database %s: %s", database.Name, err)
		}

		// Set the countCollection associated with this database
		curCountCollections[database.Name] = curCountCollection
	}

	if _, err := os.Stat(config.CountPath); os.IsNotExist(err) {
		// If the counts YAML doesn't exist, just write it out and be done
		// Write counts YAML to file
		err := writeCountCollections(config.CountPath, curCountCollections)
		if err != nil {
			log.Panicf("FATAL: Failed to write counts YAML: %s", err)
		}

	} else {
		// Otherwise, compare them with the current values and publish metrics
		// Load the last session's countCollections from the counts YAML
		lastCountCollections, err := loadCountCollections(config.CountPath)
		if err != nil {
			log.Panicf("FATAL: Failed to load counts YAML: %s", err)
		}

		// Create the countCollections map to store the difference between the two sessions' counts
		var diffCountCollections map[string]countCollection
		diffCountCollections = make(map[string]countCollection)

		for curCountCollectionName, curCountCollection := range curCountCollections {
			// Go through each countCollection from the current session
			// countCollection to store the difference between the two sessions' counts
			var diffCountCollection countCollection

			if lastCountCollection, ok := lastCountCollections[curCountCollectionName]; ok {
				// If there was a countCollection associated with this database last session, get the difference
				diffCountCollection = getCountCollectionDifference(curCountCollection, lastCountCollection)
			} else {
				// Otherwise, just continue, there is nothing to gather
				continue
			}

			// Store the difference for this database's countCollection
			diffCountCollections[curCountCollectionName] = diffCountCollection
		}

		// Put the differences as AWS metrics
		err = putAWSCountCollectionMetrics(diffCountCollections, config.AwsConfig)
		if err != nil {
			log.Printf("ERROR: Failed to push Cloudwatch metrics: %s", err)
		}

		// Overwrite the last session's counts YAML with the new one
		err = writeCountCollections(config.CountPath, curCountCollections)
		if err != nil {
			log.Panicf("FATAL: Failed to save counts YAML: %s", err)
		}
	}

	os.Exit(0)
}

// putAWSCountCollectionMetrics takes a countCollection and publishes each value as a metric on AWS CloudWatch
// Unless an explicit set of AWS configuration values is specified, it will use the normal avenues for obtaining credentials
// That is, Environment Variables -> Shared Credentials File -> EC2 IAM Role
// Unless a namespace is specified, it will put the metrics in the namepace "RowMetrics"
// It returns an error, or nil if the operation was successful
func putAWSCountCollectionMetrics(countCollections map[string]countCollection, awsConfig map[string]string) error {
	var (
		awsSession *session.Session
		err        error
		namespace  string
	)

	if awsConfig["namespace"] == "" {
		// If a namespace is not defined in the config YAML, use the default, "RowMetrics"
		namespace = "RowMetrics"
	} else {
		// Otherwise, use the namespace specified in the config YAML
		namespace = awsConfig["namespace"]
	}

	if awsConfig == nil {
		// If no credentials are explicitly specified in the config YAML, open an AWS session using the default credential provider chain
		awsSession, err = session.NewSession()
	} else {
		// Otherwise, open an AWS session using the credentials explicitly specified
		awsSession, err = session.NewSession(&aws.Config{
			Region:      aws.String(awsConfig["region"]),
			Credentials: credentials.NewStaticCredentials(awsConfig["accessKeyId"], awsConfig["secretAccessKey"], ""),
		})
	}
	if err != nil {
		return err
	}

	// Test the credentials, and fail if there are issues
	_, err = awsSession.Config.Credentials.Get()
	if err != nil {
		return err
	}

	// Create a Cloudwatch service instance using the AWS session
	cwService := cloudwatch.New(awsSession)

	for countCollectionName, countCollection := range countCollections {
		// Go through each countCollection and publish it's tableCounts as metrics
		for countName, count := range countCollection.Increment {
			// Go through each count in the Increment map, and put the cloudwatch metrics
			_, err := cwService.PutMetricData(&cloudwatch.PutMetricDataInput{
				MetricData: []*cloudwatch.MetricDatum{
					&cloudwatch.MetricDatum{
						MetricName: aws.String(countName),                    // Name of the table as MetricName
						Unit:       aws.String(cloudwatch.StandardUnitCount), // Count as the CW metric Unit
						Value:      aws.Float64(float64(count)),              // Float64 Count of the table as the Metric Value
						Dimensions: []*cloudwatch.Dimension{
							&cloudwatch.Dimension{
								Name:  aws.String("DBInstanceIdentifier"), // DBInstanceIdentifier as the metric dimension
								Value: aws.String(countCollectionName),    // Name of the database as the metric dimension's value
							},
						},
					},
				},
				Namespace: aws.String(namespace), // Put the metrics in the namespace specified
			})

			// If there is a failure in the PUT, just output it to stdout
			if err != nil {
				log.Printf("WARN: Failed to push Cloudwatch metric for table %s with count %d: %s", countName, count, err)
			} else {
				log.Printf("INFO: Pushed Cloudwatch metric for table %s with count %d", countName, count)
			}
		}

		for countName, count := range countCollection.Row {
			// Go through each count in the Increment map, and put the cloudwatch metrics
			_, err := cwService.PutMetricData(&cloudwatch.PutMetricDataInput{
				MetricData: []*cloudwatch.MetricDatum{
					&cloudwatch.MetricDatum{
						MetricName: aws.String(countName),                    // Name of the table as MetricName
						Unit:       aws.String(cloudwatch.StandardUnitCount), // Count as the CW metric Unit
						Value:      aws.Float64(float64(count)),              // Float64 Count of the table as the Metric Value
						Dimensions: []*cloudwatch.Dimension{
							&cloudwatch.Dimension{
								Name:  aws.String("DBInstanceIdentifier"), // DBInstanceIdentifier as the metric dimension
								Value: aws.String(countCollectionName),    // Name of the database as the metric dimension's value
							},
						},
					},
				},
				Namespace: aws.String(namespace), // Put the metrics in the namespace specified
			})

			// If there is a failure in the PUT, just output it to stdout
			if err != nil {
				log.Printf("ERROR: Failed to push Cloudwatch metric for table %s with difference %d: %s", countName, count, err)
			} else {
				log.Printf("INFO: Pushed Cloudwatch metric for table %s with difference %d", countName, count)
			}
		}
	}

	// Assuming no errors, return nil
	return nil
}

// getCountCollection takes a databaseConfig and then retrieves the requested table counts as a countCollection
// It returns the countCollection, as well as an error if there was any trouble retrieving the counts
func getCountCollection(dbConfig databaseConfig) (countCollection, error) {
	// countCollection to store the tableCounts
	var countCollection countCollection
	// Initialize both Increment and Row maps
	countCollection.Increment = make(map[string]int)
	countCollection.Row = make(map[string]int)

	// Database Source Name
	var dsn string

	var dbType string
	if dbConfig.Type == "" {
		// If no type is specified, set it to MySQL, since that is the default anyway
		dbType = "mysql"
	} else {
		// Otherwise, set it to the type specified in the config YAML
		dbType = dbConfig.Type
	}

	var dbSchema string
	if dbConfig.Schema == "" {
		// If no schema was explicitly defined, use a default value
		if dbType == "mysql" {
			// If it's a MySQL db, use the db name as the default schema
			dbSchema = dbConfig.Database
		} else if dbType == "postgres" {
			// If it's a PostgreSQL db, use the public schema as the default
			dbSchema = "public"
		} else {
			// Otherwise, use the db name as the default schema
			dbSchema = dbConfig.Name
		}
	} else {
		// Otherwise, use the defined schema
		dbSchema = dbConfig.Schema
	}

	if dbType == "mysql" {
		// If it's a MySQL db, generate a MySQL DSN
		dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s", dbConfig.User, dbConfig.Password, dbConfig.Host, dbConfig.Database)
	} else if dbType == "postgres" {
		// If it's a PostgreSQL db, generate a PostgreSQL DSN
		dsn = fmt.Sprintf("postgres://%s:%s@%s/%s", dbConfig.User, dbConfig.Password, dbConfig.Host, dbConfig.Database)
	} else {
		// Otherwise, generate a MySQL DSN by default as it is the most consistent
		dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s", dbConfig.User, dbConfig.Password, dbConfig.Host, dbConfig.Database)
	}

	// Create the database connection using the type and DSN
	db, err := sql.Open(dbType, dsn)
	if err != nil {
		return countCollection, err
	}
	defer db.Close()

	var (
		incrementQuery string
		incrementArgs  []interface{}
		rowQuery       string
		rowArgs        []interface{}
	)

	if dbConfig.Type == "mysql" {
		// If it's a MySQL database, generate MySQL query interfaces
		if len(dbConfig.Tables.Increment) > 0 {
			// Generate the query and slice of arguments to pull auto increment values for the specified tables
			incrementQuery, incrementArgs, err = sqlx.In("SELECT `TABLE_NAME`, `AUTO_INCREMENT` FROM information_schema.TABLES WHERE TABLE_NAME IN (?) AND TABLE_SCHEMA = ?", dbConfig.Tables.Increment, dbSchema)
			if err != nil {
				log.Printf("ERROR: Failed to assemble increment query interface: %s", err)
			}
		}

		if len(dbConfig.Tables.Row) > 0 {
			// Generate the query and slice of arguments to pull the number of rows for the specified tables
			rowQuery, rowArgs, err = sqlx.In("SELECT `TABLE_NAME`, `TABLE_ROWS` FROM information_schema.TABLES WHERE TABLE_NAME IN (?) AND TABLE_SCHEMA = ?", dbConfig.Tables.Row, dbSchema)
			if err != nil {
				log.Printf("ERROR: Failed to assemble row query interface: %s", err)
			}
		}
	} else if dbConfig.Type == "postgres" {
		// If it's a PostgreSQL datbase, generate PostgreSQL query interfaces
		// Currently, both Increment and Row use the same query, as it is non-trivial to obtain the auto-increment value
		// TODO: Figure out how to obtain auto-increment values efficiently
		if len(dbConfig.Tables.Increment) > 0 {
			// Generate the query and slice of arguments to pull the number of rows for the specified tables
			incrementQuery, incrementArgs, err = sqlx.In("SELECT relname,n_live_tup FROM pg_stat_user_tables WHERE relname IN (?) AND schemaname = ?", dbConfig.Tables.Increment, dbSchema)
			if err != nil {
				log.Printf("ERROR: Failed to assemble increment query interface: %s", err)
			}
			// Rebind the interface to use $1, $2, etc instead of ?, ?, etc as this is required by the PostgreSQL driver
			incrementQuery = sqlx.Rebind(sqlx.DOLLAR, incrementQuery)
		}

		if len(dbConfig.Tables.Row) > 0 {
			// Generate the query and slice of arguments to pull the number of rows for the specified tables
			rowQuery, rowArgs, err = sqlx.In("SELECT relname,n_live_tup FROM pg_stat_user_tables WHERE relname IN (?) AND schemaname = ?", dbConfig.Tables.Row, dbSchema)
			if err != nil {
				log.Printf("ERROR: Failed to assemble row query interface: %s", err)
			}
			// Rebind the interface to use $1, $2, etc instead of ?, ?, etc as this is required by the PostgreSQL driver
			rowQuery = sqlx.Rebind(sqlx.DOLLAR, rowQuery)
		}
	} else {
		// Otherwise, generate MySQL query interfaces by default, as MySQL is the default type anyway
		if len(dbConfig.Tables.Increment) > 0 {
			// Generate the query and slice of arguments to pull auto increment values for the specified tables
			incrementQuery, incrementArgs, err = sqlx.In("SELECT TABLE_NAME,AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_NAME IN (?) AND TABLE_SCHEMA = ?", dbConfig.Tables.Increment, dbSchema)
			if err != nil {
				log.Printf("ERROR: Failed to assemble increment query interface: %s", err)
			}
		}

		if len(dbConfig.Tables.Row) > 0 {
			// Generate the query and slice of arguments to pull the number of rows for the specified tables
			rowQuery, rowArgs, err = sqlx.In("SELECT TABLE_NAME,TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_NAME IN (?) AND TABLE_SCHEMA = ?", dbConfig.Tables.Row, dbSchema)
			if err != nil {
				log.Printf("ERROR: Failed to assemble row query interface: %s", err)
			}
		}
	}

	if incrementQuery != "" && len(incrementArgs) > 0 {
		// Query for all of the auto increment tables
		rows, err := db.Query(incrementQuery, incrementArgs...)
		if err != nil {
			log.Printf("ERROR: Failed to query database %s: %s", dbConfig.Name, err)
		} else {
			defer rows.Close()

			for rows.Next() {
				// Go through each row retrieved
				var (
					tableName  string
					tableCount int
				)

				// Assign the values to vars
				err := rows.Scan(&tableName, &tableCount)
				if err != nil {
					log.Printf("ERROR: Failed to obtain values in database %s for table %s: %s", dbConfig.Name, tableName, err)
				} else {
					// Set the count for the table key in the Increment map
					countCollection.Increment[tableName] = tableCount

					log.Printf("INFO: Obtained value in database %s for table %s with count %d", dbConfig.Name, tableName, tableCount)
				}

				// If there were any errors, output
				err = rows.Err()
				if err != nil {
					log.Printf("ERROR: Row failures for database %s: %s", dbConfig.Name, err)
				}
			}
		}
	}

	if rowQuery != "" && len(rowArgs) > 0 {
		// If the number of row tables isn't empty, query
		// Query for all of the row count tables
		rows, err := db.Query(rowQuery, rowArgs...)
		if err != nil {
			log.Printf("ERROR: Failed to query database %s: %s", dbConfig.Name, err)
		} else {
			defer rows.Close()

			for rows.Next() {
				// Go through each row retrieved
				var (
					tableName  string
					tableCount int
				)

				// Assign the values to vars
				err := rows.Scan(&tableName, &tableCount)
				if err != nil {
					log.Printf("ERROR: Failed to obtain values in database %s for table %s: %s", dbConfig.Name, tableName, err)
				} else {
					// Set the count for the table key in the Row map
					countCollection.Row[tableName] = tableCount

					log.Printf("INFO: Obtained value in database %s for table %s with count %d", dbConfig.Name, tableName, tableCount)
				}

				// If there were any errors, output
				err = rows.Err()
				if err != nil {
					log.Printf("ERROR: Row failures for database %s: %s", dbConfig.Name, err)
				}
			}
		}
	}

	// Assuming no fatal errors, return nil
	return countCollection, nil
}

// getCountCollectionDifference takes two countCollections, subtracts the counts, returns the difference
// It returns the difference as a countCollection
func getCountCollectionDifference(minuend countCollection, subtrahend countCollection) countCollection {
	// Create the countCollection to store the difference
	var difference countCollection
	// Initialize both Increment and Row maps
	difference.Increment = make(map[string]int)
	difference.Row = make(map[string]int)

	for minCountName, minCount := range minuend.Increment {
		// Go through each count in the minuend Increment
		if subCount, ok := subtrahend.Increment[minCountName]; ok {
			// If there is a corresponding count in the subtrahend, subtract and set the value in the difference countCollection
			difference.Increment[minCountName] = minCount - subCount
		} else {
			// Otherwise, just set the difference to 0, since this is the first time this count has been counted
			difference.Increment[minCountName] = 0
		}
	}

	for minCountName, minCount := range minuend.Row {
		// Go through each count in the minuend Row
		if subCount, ok := subtrahend.Row[minCountName]; ok {
			// If there is a corresponding count in the subtrahend, subtract and set the value in the difference countCollection
			difference.Row[minCountName] = minCount - subCount
		} else {
			// Otherwise, just set the difference to 0, since this is the first time this count has been counted
			difference.Row[minCountName] = 0
		}
	}

	// Return the difference countCollection
	return difference
}

// loadApplicationConfig loads the config YAML file from a filename and maps the values to an applicationConfig
// It returns an applicationConfig with the mapped values, as well as an error
func loadApplicationConfig(fileName string) (applicationConfig, error) {
	var config applicationConfig

	// Load the application config YAML file
	configSource, err := ioutil.ReadFile(fileName)
	if err != nil {
		return config, err
	}

	// Map the YAML file to the applicationConfig object
	err = yaml.Unmarshal(configSource, &config)
	if err != nil {
		return config, err
	}

	// Assuming no errors, return the applicationConfig and nil
	return config, nil
}

// loadCountCollections loads a map of countCollections from a YAML file
// It returns a map composed of each database and it's associated countCollection, as well as an error
func loadCountCollections(fileName string) (map[string]countCollection, error) {
	var countCollections map[string]countCollection

	// Load the last countCollections YAML file
	countCollectionsSource, err := ioutil.ReadFile(fileName)
	if err != nil {
		return countCollections, err
	}

	// Map the YAML file to the map of countCollections
	err = yaml.Unmarshal(countCollectionsSource, &countCollections)
	if err != nil {
		return countCollections, err
	}

	// Assuming no errors, return the countCollections and nil
	return countCollections, nil
}

// writeCountCollections writes a map of countCollections to an exported YAML file
// It returns an error if any issues were encountered
func writeCountCollections(fileName string, countCollections map[string]countCollection) error {
	// Take the countCollections and export it into a YAML file
	countCollectionsYaml, err := yaml.Marshal(&countCollections)
	if err != nil {
		return err
	}

	// Create and open the counts YAML file
	countCollectionsFile, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer countCollectionsFile.Close()

	// Output the generated YAML into the file
	fmt.Fprintf(countCollectionsFile, string(countCollectionsYaml))

	// Assuming no errors, return nil
	return nil
}
