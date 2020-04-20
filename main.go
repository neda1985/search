package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"log"
	"net/http"
	"os"
)

var host = flag.String("host", os.Getenv("ELASTIC_HOST"), "The elastic host to use")
var index = flag.String("index", os.Getenv("INDEX_NAME"), "The index name")
var port = flag.String("port", os.Getenv("PORT"), "port")
var es *elasticsearch.Client

func main() {
	flag.Parse()
	var err error
	es, err = elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{
			*host,
		},
	})
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}
	http.HandleFunc("/index/song", indexSong)
	http.HandleFunc("/index/song/get", getSongs)
	log.Fatal(http.ListenAndServe(*port, nil))
}

func indexSong(w http.ResponseWriter, r *http.Request) {
	req := esapi.IndexRequest{
		Index:   *index,
		Body:    r.Body,
		Refresh: "true",
	}
	//checkClientError()
	res, err := req.Do(r.Context(), es)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Print(res.String())
	} else {
		var req map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&req); err != nil {
			log.Printf("Error parsing the response body: %s", err)
		} else {
			log.Printf("[%s] %s; version=%d", res.Header, req["result"], int(req["_version"].(float64)))
		}
	}
}

type errorResponse struct {
	Error map[string]interface{}
}

func getSongs(w http.ResponseWriter, r *http.Request) {
	moods := r.URL.Query().Get("moods")
	genre := r.URL.Query().Get("genre")
	id := r.URL.Query().Get("id")
	var (
		m map[string]interface{}
	)
	//checkClientError()

	var buf bytes.Buffer
	query := queryBuilder(moods, genre, id)
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Fatalf("Error encoding query: %s", err)
	}

	// Perform the search request.
	response, err := es.Search(
		es.Search.WithContext(r.Context()),
		es.Search.WithIndex(*index),
		es.Search.WithBody(&buf),
		es.Search.WithTrackTotalHits(true),
		es.Search.WithPretty(),
	)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer response.Body.Close()

	if response.IsError() {
		var e errorResponse
		if err := json.NewDecoder(response.Body).Decode(&e); err != nil {
			log.Fatalf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and error information.
			log.Fatalf("[%s] %s: %s",
				response.Status(),
				e.Error["type"],
				e.Error["reason"],
			)
		}
	}

	if err := json.NewDecoder(response.Body).Decode(&m); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}
	// Print the response status, number of results, and request duration.
	log.Printf(
		"[%s] %d hits; took: %dms",
		response.Status(),
		int(m["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64)),
		int(m["took"].(float64)),
	)
	// Print the ID and document source for each hit.
	for _, hit := range m["hits"].(map[string]interface{})["hits"].([]interface{}) {
		log.Printf(" * ID=%s, %s", hit.(map[string]interface{})["_id"], hit.(map[string]interface{})["_source"])
	}
}

type match struct {
	Match map[string]string `json:"match"`
}

func queryBuilder(moods string, genre string, id string) map[string]interface{} {

	m := make(map[string]interface{})

	if len(genre) == 0 && len(moods) == 0 && len(id) == 0 {
		m["query"] = map[string]interface{}{
			"match_all": struct{}{},
		}
		return m
	}
	must := make([]match, 0)
	if len(genre) > 0 {
		must = append(must, match{Match: map[string]string{"genre": genre}})
	}
	if len(moods) > 0 {
		must = append(must, match{Match: map[string]string{"moods": moods}})
	}
	if len(id) > 0 {
		must = append(must, match{Match: map[string]string{"id": id}})
	}

	m["query"] = map[string]interface{}{
		"bool": map[string]interface{}{
			"must": must,
		},
	}
	return m
}
