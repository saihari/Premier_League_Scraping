package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/iancoleman/strcase"

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
	router.GET("/PL/:team", app.GetTeamPerformance)
	router.Run("localhost:8080")
}

func (app *App) GetTeamPerformance(c *gin.Context) {
	team := c.Param("team")

	fmt.Println("Querying For:", strcase.ToCamel(team))

	// rows, err := app.DB.Query(`Select * from regular_season where "Squad" = $1`, strcase.ToCamel(team))
	query := fmt.Sprintf(`Select * from regular_season where "Squad" = '%s';`, strcase.ToCamel(team))
	fmt.Println(query)
	rows, err := app.DB.Query(query)
	if err != nil {

		c.JSON(500, gin.H{"error": "Failed to retrieve data"})
		return
	}
	defer rows.Close()

	// Create a map to hold the rows
	users := []map[string]interface{}{}

	// Iterate over the rows
	for rows.Next() {

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			log.Fatal(err)
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
			log.Fatal(err)
		}

		// Create a map to hold the row data
		rowData := map[string]interface{}{}
		for i, column := range columns {
			rowData[column] = values[i]
		}

		// Append the row data to the users slice
		users = append(users, rowData)
	}

	// Check for errors during iteration
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	// Convert the users slice to JSON
	jsonData, err := json.Marshal(users)
	if err != nil {
		log.Fatal(err)
	}

	// Set the response content type to JSON
	c.Header("Content-Type", "application/json")

	// Send the JSON data as the response
	c.IndentedJSON(http.StatusOK, string(jsonData))
}
