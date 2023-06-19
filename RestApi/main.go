package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	_ "github.com/lib/pq"

	"github.com/gin-gonic/gin"
)

type App struct {
	DB *sql.DB
}

const (
	database = "football-db"
	user     = "user"
	password = "password"
	host     = "192.168.59.101"
	port     = "30432"
)

func runQuery(rows *sql.Rows) (interface{}, error) {

	// Create a map to hold the rows
	stats := []map[string]interface{}{}

	// Iterate over the rows
	for rows.Next() {

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		// Create a slice to hold column values
		values := make([]interface{}, len(columns))
		columnPointers := make([]interface{}, len(columns))
		for i := range columns {
			columnPointers[i] = &values[i]
		}

		// Scan the row values into the columnPointers slice
		err = rows.Scan(columnPointers...)
		if err != nil {
			return nil, err
		}

		// Create a map to hold the row data
		rowData := map[string]interface{}{}
		for i, column := range columns {
			rowData[column] = values[i]
		}

		// Append the row data to the stats slice
		stats = append(stats, rowData)
	}

	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return stats, nil
}

func main() {

	postgresqlDbInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, database)

	db, err := sql.Open("postgres", postgresqlDbInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	// Initialize the application
	app := &App{
		DB: db,
	}

	fmt.Println("Successfully connected to database!")

	router := gin.Default()
	router.GET("/PL/stats/:team", app.GetTeamStats)
	router.GET("/PL/ratings/:team", app.GetTeamRatings)
	router.Run("localhost:8080")
}

func (app *App) GetTeamStats(c *gin.Context) {
	team := c.Param("team")

	fmt.Println("Querying Stats For:", strings.ToLower(team))

	// Run query and Get Data
	rows, err := app.DB.Query(`Select * from regular_season where squad = $1`, strings.ToLower(strings.ReplaceAll(team, "'", "''")))
	if err != nil {
		log.Fatal(err)
		c.IndentedJSON(http.StatusInternalServerError, "Failed to Query Data")
	}

	data, err := runQuery(rows)
	if err != nil {
		log.Fatal(err)
		c.IndentedJSON(http.StatusInternalServerError, "Data Error")
	}

	rows.Close()

	// Convert the data slice to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Fatal(err)
	}

	// Set the response content type to JSON
	c.Header("Content-Type", "application/json")

	// Send the JSON data as the response
	c.IndentedJSON(http.StatusOK, string(jsonData))
}

func (app *App) GetTeamRatings(c *gin.Context) {
	team := c.Param("team")

	fmt.Println("Querying Ratings For:", strings.ToLower(team))

	// Run query and Get Data
	rows, err := app.DB.Query(`Select * from ratings where squad = $1`, strings.ToLower(team))
	if err != nil {
		log.Fatal(err)
		c.IndentedJSON(http.StatusInternalServerError, "Failed to Query Data")
	}

	data, err := runQuery(rows)
	if err != nil {
		log.Fatal(err)
		c.IndentedJSON(http.StatusInternalServerError, "Data Error")
	}

	rows.Close()

	fmt.Println(data)

	// Convert the data slice to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Fatal(err)
	}

	// Set the response content type to JSON
	c.Header("Content-Type", "application/json")

	// Send the JSON data as the response
	c.IndentedJSON(http.StatusOK, string(jsonData))
}
