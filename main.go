package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"strconv"

	"github.com/olivere/elastic"
)

const (
	POST_INDEX = "post"
	DISTANCE   = "200km"

	ES_URL = "http://10.128.0.2:9200"
)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	User     string   `json:"user"`
	Message  string   `json:"message"`
	Location Location `json:"location"`
	Url      string   `json:"url"`
	Type     string   `json:"type"`
	Face     float32  `json:"face"`
}

func main() {
	fmt.Println("started-service")
	http.HandleFunc("/post", handlerPost)
	http.HandleFunc("/search", handlerSearch)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handlerPost(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one post request")
	decoder := json.NewDecoder(r.Body)
	var p Post
	if err := decoder.Decode(&p); err != nil {
		panic(err)
	}
	fmt.Fprintf(w, "Post received: %s\n", p.Message)
}

// search based on lat and lon
func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received a search request")
	w.Header().Set("Content-Type", "application/json")
	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}
	fmt.Println("Range is ", ran)

	query := elastic.NewGeoDistanceQuery("location")
	query = query.Distance(ran).Lat(lat).Lon(lon)
	searchResult, err := ReadFromES(query, POST_INDEX)
	if err != nil {
		errMsg := "Failed to read post from Elasticsearch"
		http.Error(w, errMsg, http.StatusInternalServerError)
		fmt.Print("%s. %v.\n", errMsg, err)
		return
	}

	posts := getPostsFromSearchResult(searchResult)
	js, err := json.Marshal(posts)
	if err != nil {
		errMsg := "Failed to parse JSON into JSON format"
		http.Error(w, errMsg, http.StatusInternalServerError)
		fmt.Print("%s. %v.\n", errMsg, err)
		return
	}
	w.Write(js)
}

func ReadFromES(query elastic.Query, index string) (*elastic.SearchResult, error) {
	client, err := elastic.NewClient(elastic.SetURL(ES_URL))
	if err != nil {
		return nil, err
	}
	searchResult, err := client.Search().
		Index(index).
		Query(query).
		Pretty(true).
		Do(context.Background())
	if err != nil {
		return nil, err
	}
	return searchResult, nil
}

func getPostsFromSearchResult(searchResult *elastic.SearchResult) []Post {
	var pType Post
	var posts []Post
	for _, item := range searchResult.Each(reflect.TypeOf(pType)) {
		if p, ok := item.(Post); ok {
			posts = append(posts, p)
		}
	}
	return posts
}
